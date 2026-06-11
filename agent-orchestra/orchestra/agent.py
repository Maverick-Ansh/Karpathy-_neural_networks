"""
agent.py — THE AGENT LOOP. If you read one file closely, read this one.

THE CONCEPT
-----------
Strip away the hype and an "agent" is a *loop around a brain*:

    forever:
        task   = await inbox.get()          # PERCEIVE  (wait for work)
        result = think(task)                # DECIDE    (the LLM call)
        await outbox.put(result)            # ACT/REPORT (tell the world)

That's it. That's the whole secret. Claude Code, AutoGPT, swarm frameworks —
under the costumes, every agent is some elaboration of these three lines.
Our 25 agents are 25 instances of this loop running concurrently on ONE
thread, interleaved by Python's asyncio event loop: whenever a coroutine
hits `await` (waiting on a queue, sleeping, waiting for the network), the
event loop parks it and runs whoever else is ready. LLM work is almost all
network-waiting, which is why async — not threads, not processes — is the
right concurrency tool for this job.

THE THREE RULES OF A WELL-BEHAVED WORKER
----------------------------------------
1. NEVER DIE. Every exception is caught and converted into a failed Result.
   A worker that crashes silently strands its queue and deadlocks the system
   (the supervisor counts on one Result per Task — see messages.py).
2. NEVER HOG. The global semaphore caps simultaneous LLM calls; the timeout
   guarantees no call can hold a slot hostage forever.
3. NEVER DECIDE. Workers don't retry, don't route, don't spawn follow-ups.
   They do the work and report. ALL policy lives in the supervisor — this
   separation (dumb workers, smart coordinator) is what makes the system
   easy to reason about. Erlang calls this a supervision tree; the worker's
   motto there is "let it crash" — ours merely "let it fail (loudly)".
"""

from __future__ import annotations

import asyncio
import time

from .config import RoleSpec, Settings
from .log import Console
from .messages import Mailbox, POISON, Result, Task


class Agent:
    def __init__(self, name: str, role: str, spec: RoleSpec, llm,
                 inbox: Mailbox, outbox: asyncio.Queue, throttle: asyncio.Semaphore,
                 console: Console, settings: Settings) -> None:
        self.name = name              # e.g. "researcher-3"
        self.role = role
        self.spec = spec
        self.llm = llm
        self.inbox = inbox            # SHARED with all agents of this role —
                                      # whoever is free grabs the next task
                                      # ("competing consumers" pattern)
        self.outbox = outbox          # one global results queue -> supervisor
        self.throttle = throttle      # global concurrency cap (shared by all 25)
        self.console = console
        self.settings = settings
        # Per-agent counters for the end-of-run stats table.
        self.tasks_done = 0
        self.tasks_failed = 0
        self.busy_seconds = 0.0

    # ------------------------------------------------------------------ loop

    async def run(self) -> None:
        """The heartbeat. Runs until a poison pill arrives."""
        while True:
            task = await self.inbox.get()           # PERCEIVE — parked here while idle

            if task is POISON:                      # graceful shutdown signal
                self.console.line(self.name, "PILL", "shutting down")
                return

            self.console.line(self.name, "START",
                              f"{task.stage}: {task.description[:70]}")
            t0 = time.monotonic()
            try:
                # NEVER HOG: the semaphore admits at most N concurrent LLM
                # calls system-wide; the timeout bounds each one. `async with`
                # acquires on entry and ALWAYS releases on exit, even when the
                # call inside explodes.
                async with self.throttle:
                    output = await asyncio.wait_for(
                        self.think(task), timeout=self.settings.task_timeout
                    )
                elapsed = time.monotonic() - t0
                self.tasks_done += 1
                self.busy_seconds += elapsed
                result = Result(task=task, ok=True, output=output,
                                agent_name=self.name, elapsed=elapsed)
                self.console.line(self.name, "DONE", f"{task.stage} in {elapsed:.1f}s")

            except Exception as e:                  # NEVER DIE — see rule 1
                elapsed = time.monotonic() - t0
                self.tasks_failed += 1
                self.busy_seconds += elapsed
                err = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
                result = Result(task=task, ok=False, error=err,
                                agent_name=self.name, elapsed=elapsed)
                self.console.line(self.name, "FAIL", f"{task.stage} — {err}")

            await self.outbox.put(result)           # REPORT — and loop around

    # ----------------------------------------------------------------- brain

    async def think(self, task: Task) -> str:
        """One LLM call: persona as the system prompt, task as the user turn.

        Note what the agent does NOT do here: it doesn't read the blackboard,
        doesn't talk to other agents, doesn't know the pipeline exists. Its
        entire world is the Task it was handed. That isolation is what makes
        agents swappable, testable, and shippable to other machines.
        """
        prompt = self.render_prompt(task)
        return await self.llm.complete(
            system=self.spec.system,
            prompt=prompt,
            tier=self.spec.tier,
            meta={                                   # used by MockLLM to fake
                "stage": task.stage,                 # plausible, data-derived
                "topic": task.context.get("topic", ""),       # output
                "mission": task.context.get("mission", ""),
                "artifacts": task.context.get("artifacts", {}),
            },
        )

    def render_prompt(self, task: Task) -> str:
        """Assemble the user prompt from the task + upstream artifacts.

        Upstream artifacts are TRUNCATED to ctx_snippet_chars. Context windows
        are a budget: a pipeline that naively forwards everything it ever saw
        grows without bound, gets slow, gets expensive, and eventually
        overflows the model's context. Summarize-or-truncate at every hop.
        """
        parts = [f"MISSION: {task.context.get('mission', '(none)')}",
                 f"YOUR TASK ({task.stage}): {task.description}"]
        artifacts = task.context.get("artifacts", {})
        if artifacts:
            parts.append("\nUPSTREAM MATERIAL:")
            for key, text in sorted(artifacts.items()):
                snippet = text[: self.settings.ctx_snippet_chars]
                parts.append(f"--- {key} ---\n{snippet}")
        else:
            parts.append("\n(no upstream material — you are first in the chain)")
        return "\n".join(parts)
