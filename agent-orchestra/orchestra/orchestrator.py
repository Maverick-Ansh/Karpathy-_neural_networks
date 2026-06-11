"""
orchestrator.py — the CONDUCTOR. Routing, supervision, retries, phases.

THE CONCEPT
-----------
The orchestrator owns all the *policy* the workers were forbidden to have
(agent.py, rule 3). It is three cooperating routines on one event loop:

  THE DISPATCH PATH (submit) — route each Task to the Mailbox of its role.
      Role-based routing is the simplest useful routing strategy; others you
      could swap in: round-robin, least-loaded, broadcast, content-based.

  THE SUPERVISOR LOOP (supervise) — the only consumer of the results queue,
      and the only writer of shared state. For every Result it decides:
        success  -> archive the artifact, consult WORKFLOW, spawn children
        failure  -> retry with exponential backoff + jitter, or dead-letter
      Because ONE loop makes every decision, there are no races to reason
      about: the system's brain is single-threaded even though its hands
      (the 25 agents) are concurrent. This is the actor-model insight.

  THE HEARTBEAT (heartbeat) — a background routine that periodically prints
      queue depths and counters. Concurrency you can't see doesn't exist.

TERMINATION — the bookkeeping invariant
---------------------------------------
How do you know a swarm is *finished*? Queues-empty is NOT enough: an agent
might be mid-task and about to spawn three children into those empty queues.
We count instead:

    open_tasks += 1   on submit
    open_tasks -= 1   when a task reaches a TERMINAL state (DONE or DEAD)

A task awaiting retry is NOT terminal — it stays counted, which is precisely
what makes the count race-free: the phase can never advance while a retry
timer is pending. When open_tasks hits 0, the current phase has fully
drained, and the supervisor advances the phase machine:

    pipeline (plan→research→analysis/factcheck→critique)
      └─ drain ─▶ synthesis (2 competing drafts)
                    └─ drain ─▶ edit (merge)
                                  └─ drain ─▶ archive ──▶ DONE

(The other classic termination tool is asyncio.Queue.join()/task_done() —
fine for one queue, unwieldy for a graph of queues that spawn into each
other. Counting open work is the standard generalisation.)
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import time
from pathlib import Path

from .agent import Agent
from .blackboard import Blackboard
from .config import PRIORITY, ROLES, Settings, WORKFLOW
from .log import Console
from .messages import Mailbox, POISON, Result, Task, TaskStatus


def slugify(text: str, limit: int = 48) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return text[:limit].rstrip("-") or "untitled"


class Orchestrator:
    def __init__(self, llm, settings: Settings) -> None:
        self.llm = llm
        self.settings = settings
        self.console = Console(use_color=settings.color)

        # --- the plumbing -------------------------------------------------
        # One Mailbox per ROLE (not per agent): agents of a role compete for
        # tasks from their shared queue. One global results queue feeds the
        # supervisor. This star topology (everything through the middle) is
        # the easiest to debug; peer-to-peer agent meshes look cool and are
        # miserable to operate.
        self.role_queues: dict[str, Mailbox] = {role: Mailbox() for role in ROLES}
        self.results: asyncio.Queue[Result] = asyncio.Queue()
        self.blackboard = Blackboard()

        # Global LLM concurrency cap — see Settings.max_concurrency.
        self.throttle = asyncio.Semaphore(settings.max_concurrency)

        # --- hire the orchestra (25 agents) --------------------------------
        self.agents: list[Agent] = []
        for role, spec in ROLES.items():
            for i in range(1, spec.count + 1):
                self.agents.append(Agent(
                    name=f"{role}-{i}", role=role, spec=spec, llm=llm,
                    inbox=self.role_queues[role], outbox=self.results,
                    throttle=self.throttle, console=self.console,
                    settings=settings,
                ))

        # --- supervisor state ----------------------------------------------
        self.open_tasks = 0                 # the termination counter (docstring!)
        self.phase = "pipeline"
        self.done = asyncio.Event()         # set when the whole run is finished
        self.ledger: dict[str, dict] = {}   # task id -> audit record (machine-readable)
        self.dead_letters: list[dict] = []  # tasks that exhausted their retries
        self.retries = 0
        self.completed = 0
        self.rng = random.Random(settings.seed)   # for retry jitter
        self._timers: set[asyncio.Task] = set()   # pending delayed-requeue timers
        self.started_at = time.monotonic()

    # ------------------------------------------------------------- dispatch

    def submit(self, task: Task) -> None:
        """Route a task to its role's mailbox and open the books on it."""
        self.open_tasks += 1
        self.ledger[task.id] = {
            "id": task.id, "role": task.role, "stage": task.stage,
            "description": task.description, "status": TaskStatus.PENDING,
            "attempts": 0, "agent": None, "elapsed": None,
            "parent_id": task.parent_id,
        }
        self.role_queues[task.role].put_nowait(task, priority=task.priority)
        self.console.line("supervisor", "SPAWN",
                          f"{task.stage} -> {task.role}: {task.description[:60]}")

    def _make_task(self, *, role: str, stage: str, description: str,
                   topic: str = "", artifacts: dict | None = None,
                   parent: Task | None = None) -> Task:
        return Task(
            role=role, stage=stage, description=description,
            priority=PRIORITY.get(stage, 5),
            parent_id=parent.id if parent else None,
            context={
                "mission": self.settings.mission,
                "topic": topic,
                "artifacts": artifacts or {},
            },
        )

    # ------------------------------------------------------------ lifecycle

    async def run(self) -> str:
        """Start everything, kick off the mission, wait for DONE, shut down."""
        self.console.line("orchestra", "PHASE",
                          f"25 agents hired. mission: {self.settings.mission!r}")

        # Start 25 agent loops + supervisor + heartbeat as concurrent tasks
        # on THIS event loop. Nothing runs yet beyond this line — coroutines
        # only progress when we await and the loop gets control.
        runners = [asyncio.create_task(a.run(), name=a.name) for a in self.agents]
        supervisor = asyncio.create_task(self.supervise(), name="supervisor")
        pulse = (asyncio.create_task(self.heartbeat(), name="heartbeat")
                 if self.settings.pulse_every else None)

        # Task #1: ask the planner to decompose the mission. Everything else
        # cascades from this one seed task via the WORKFLOW table.
        self.submit(self._make_task(
            role="planner", stage="plan",
            description=f"Decompose into ~{self.settings.fanout} research subtopics",
        ))

        await self.done.wait()      # park here until the phase machine finishes

        # --- graceful shutdown: poison pills, one per agent ----------------
        # Priority 999 = pills sort behind any real work (there is none left,
        # but belt and braces). Each pill stops exactly one agent.
        for role, spec in ROLES.items():
            for _ in range(spec.count):
                self.role_queues[role].put_nowait(POISON, priority=999)
        await asyncio.gather(*runners)            # wait for all 25 clean exits

        supervisor.cancel()                       # nothing left to supervise
        if pulse:
            pulse.cancel()
        for t in list(self._timers):              # defensive; should be empty
            t.cancel()

        report = self.blackboard.get("edit/final-report") \
            or "(mission failed before a report was produced)"
        self._print_stats()
        self._write_outputs(report)
        return report

    # ----------------------------------------------------------- supervisor

    async def supervise(self) -> None:
        """THE SUPERVISOR LOOP — sole consumer of results, maker of all
        decisions. Read together with the module docstring."""
        while True:
            result = await self.results.get()
            task = result.task
            rec = self.ledger[task.id]
            rec.update(agent=result.agent_name, elapsed=round(result.elapsed, 2),
                       attempts=task.attempts + 1)

            if result.ok:
                self.completed += 1
                rec["status"] = TaskStatus.DONE
                self._on_success(result)
                self._finalize(task)
                continue

            # ---- failure path: retry with backoff, or dead-letter ---------
            if task.attempts + 1 < self.settings.max_attempts:
                task.attempts += 1
                # EXPONENTIAL BACKOFF: 0.5s, 1s, 2s, ... doubling per attempt,
                # times random JITTER (0.5x-1.5x). Backoff gives a struggling
                # dependency room to recover; jitter prevents the "thundering
                # herd" where every failed task retries at the same instant
                # and knocks the service over again in lockstep.
                delay = (self.settings.retry_base * (2 ** (task.attempts - 1))
                         * self.rng.uniform(0.5, 1.5))
                self.retries += 1
                rec["status"] = TaskStatus.PENDING
                self.console.line(
                    "supervisor", "RETRY",
                    f"{task.stage} attempt {task.attempts + 1}/"
                    f"{self.settings.max_attempts} in {delay:.1f}s ({result.error})")
                # The delayed requeue runs as its own tiny background task so
                # the supervisor never sleeps — it must stay responsive to the
                # other 24 agents' results. NOTE: the task stays counted in
                # open_tasks the whole time (termination invariant).
                timer = asyncio.create_task(self._requeue_later(task, delay))
                self._timers.add(timer)
                timer.add_done_callback(self._timers.discard)
            else:
                # DEAD LETTER: we tried, we're done trying. Park it for the
                # post-mortem instead of retrying forever (a "poison message"
                # that always fails would otherwise loop eternally). The
                # pipeline DEGRADES GRACEFULLY: this branch of the task tree
                # is pruned; downstream stages simply see fewer artifacts.
                rec["status"] = TaskStatus.DEAD
                self.dead_letters.append(rec)
                self.console.line("supervisor", "DEAD",
                                  f"{task.stage} '{task.description[:40]}' "
                                  f"after {self.settings.max_attempts} attempts")
                self._finalize(task)

    async def _requeue_later(self, task: Task, delay: float) -> None:
        await asyncio.sleep(delay)
        self.ledger[task.id]["status"] = TaskStatus.PENDING
        self.role_queues[task.role].put_nowait(task, priority=task.priority)

    def _finalize(self, task: Task) -> None:
        """A task reached a terminal state (DONE or DEAD). Close the books;
        if that drained the phase, advance the phase machine."""
        self.open_tasks -= 1
        if self.open_tasks == 0:
            self._advance_phase()

    # ------------------------------------------------------------- workflow

    def _on_success(self, result: Result) -> None:
        """Archive the artifact, then consult the WORKFLOW table to spawn
        children. This is EVENT-DRIVEN CHAINING: nobody scripts 'now run all
        analyses' — each research success independently triggers its own
        analysis, so stages overlap naturally (analysis #1 can start while
        research #7 is still queued)."""
        task = result.task
        topic = task.context.get("topic", "") or task.stage
        key = f"{task.stage}/{slugify(topic)}"
        self.blackboard.put(key, result.output)

        if task.stage == "plan":
            self._fan_out_research(result)
            return

        for child_stage, child_role in WORKFLOW.get(task.stage, []):
            self.submit(self._make_task(
                role=child_role, stage=child_stage,
                description=f"{child_stage} of the {task.stage} notes on '{topic[:50]}'",
                topic=topic,
                artifacts={key: result.output},   # the child sees its parent's work
                parent=task,
            ))

    def _fan_out_research(self, result: Result) -> None:
        """FAN-OUT: one plan becomes N parallel research tasks."""
        topics = self._parse_plan(result.output)[: self.settings.fanout]
        self.console.line("supervisor", "PHASE",
                          f"plan accepted: fanning out {len(topics)} research tasks "
                          f"across {ROLES['researcher'].count} researchers")
        for topic in topics:
            self.submit(self._make_task(
                role="researcher", stage="research",
                description=f"Research: {topic}", topic=topic, parent=result.task,
            ))

    @staticmethod
    def _parse_plan(text: str) -> list[str]:
        """DEFENSIVE PARSING. We *asked* the planner for a JSON array, but
        LLMs are probabilistic: sooner or later you get markdown fences, a
        chatty preamble, or a bullet list. Production agent code always has
        a fallback parse — never let one malformed answer kill the mission."""
        try:
            data = json.loads(text.strip().strip("`"))
            if isinstance(data, list):
                return [str(t).strip() for t in data if str(t).strip()]
        except json.JSONDecodeError:
            pass
        # Fallback: treat each non-empty line as a topic, stripping bullets.
        topics = []
        for line in text.splitlines():
            line = line.strip().lstrip("-*0123456789. ").strip()
            if line and not line.startswith(("[", "]", "```")):
                topics.append(line)
        return topics

    def _advance_phase(self) -> None:
        """The PHASE MACHINE. Called exactly when open_tasks hits 0 — i.e. the
        current phase has fully drained (fan-IN barrier). Each arm submits the
        next phase's tasks, which makes open_tasks > 0 again."""
        if self.phase == "pipeline":
            if not self.blackboard.keys("research/"):
                self.console.line("orchestra", "FAIL",
                                  "no research survived — aborting mission")
                self.done.set()
                return
            self.phase = "synthesis"
            self.console.line("supervisor", "PHASE",
                              "pipeline drained -> synthesis: 2 writers draft in parallel")
            # BEST-OF-N: two writers get the SAME material and compete. The
            # editor picks/merges. Sampling multiple candidates and selecting
            # is one of the simplest reliable quality boosts in agent design.
            material = {**self.blackboard.collect("analysis/"),
                        **self.blackboard.collect("critique/"),
                        **self.blackboard.collect("factcheck/")}
            for i in (1, 2):
                self.submit(self._make_task(
                    role="writer", stage="draft",
                    description=f"Write competing draft #{i} of the final report",
                    topic=f"draft-{i}", artifacts=material,
                ))

        elif self.phase == "synthesis":
            self.phase = "edit"
            self.console.line("supervisor", "PHASE",
                              "drafts in -> edit: editor merges the competition")
            self.submit(self._make_task(
                role="editor", stage="edit",
                description="Merge the competing drafts into the final report",
                topic="final-report", artifacts=self.blackboard.collect("draft/"),
            ))

        elif self.phase == "edit":
            self.phase = "archive"
            self.console.line("supervisor", "PHASE", "report done -> archive")
            self.submit(self._make_task(
                role="librarian", stage="archive",
                description="Index every artifact produced this run",
                topic="index",
                artifacts={"__keys__": "\n".join(self.blackboard.keys())},
            ))

        elif self.phase == "archive":
            self.phase = "done"
            self.console.line("orchestra", "PHASE", "mission complete")
            self.done.set()

    # ------------------------------------------------------------ heartbeat

    async def heartbeat(self) -> None:
        """A background ROUTINE (periodic coroutine). Real orchestrators run
        several of these: metrics flushers, watchdogs (kill tasks running too
        long), autoscalers (queue deep? hire more workers). Ours just makes
        the invisible visible: watch q[researcher] absorb the fan-out burst
        and drain — that's backpressure breathing."""
        while not self.done.is_set():
            await asyncio.sleep(self.settings.pulse_every)
            if self.done.is_set():
                break
            depths = " ".join(f"q[{role}]={q.qsize()}"
                              for role, q in self.role_queues.items() if q.qsize())
            self.console.line("supervisor", "PULSE",
                              f"open={self.open_tasks} done={self.completed} "
                              f"retries={self.retries} dead={len(self.dead_letters)} "
                              f"{depths}")

    # ---------------------------------------------------------------- stats

    def _print_stats(self) -> None:
        wall = time.monotonic() - self.started_at
        by_role: dict[str, dict] = {}
        for a in self.agents:
            r = by_role.setdefault(a.role, {"agents": 0, "done": 0, "fail": 0, "busy": 0.0})
            r["agents"] += 1
            r["done"] += a.tasks_done
            r["fail"] += a.tasks_failed
            r["busy"] += a.busy_seconds

        print("\n=== run statistics " + "=" * 47)
        print(f"{'role':<14}{'agents':>7}{'done':>6}{'fail':>6}{'avg think':>11}")
        for role, r in by_role.items():
            attempts = r["done"] + r["fail"]
            avg = r["busy"] / attempts if attempts else 0.0
            print(f"{role:<14}{r['agents']:>7}{r['done']:>6}{r['fail']:>6}{avg:>10.2f}s")
        print("-" * 66)
        print(f"tasks completed: {self.completed}   retries: {self.retries}   "
              f"dead letters: {len(self.dead_letters)}   wall time: {wall:.1f}s")

        # Honest accounting for the brain layer, whichever brain it was.
        if hasattr(self.llm, "injected_failures"):       # MockLLM
            print(f"mock llm: {self.llm.calls} calls, "
                  f"{self.llm.injected_failures} injected failures, cost $0.00")
        elif hasattr(self.llm, "cost_estimate"):         # AnthropicLLM
            total, detail = self.llm.cost_estimate()
            print(f"claude api: {self.llm.calls} calls — {detail} — total ≈ ${total:.4f}")

        # The busy/wall ratio is the payoff of async: 25 agents' combined
        # think-time squeezed into a much shorter wall-clock window.
        total_busy = sum(a.busy_seconds for a in self.agents)
        if wall > 0:
            print(f"concurrency payoff: {total_busy:.1f}s of agent think-time "
                  f"in {wall:.1f}s of wall time ({total_busy / wall:.1f}x)")

    def _write_outputs(self, report: str) -> None:
        """Logs are for humans; the LEDGER is for machines (and post-mortems).
        Every task's full lifecycle — who ran it, how many attempts, how long —
        lands in runs/<timestamp>/ledger.json. Open it after a run."""
        out = Path(self.settings.runs_dir) / time.strftime("%Y%m%d-%H%M%S")
        out.mkdir(parents=True, exist_ok=True)
        (out / "report.md").write_text(report)
        (out / "ledger.json").write_text(json.dumps({
            "mission": self.settings.mission,
            "mode": "real" if self.settings.real else "mock",
            "completed": self.completed,
            "retries": self.retries,
            "dead_letters": len(self.dead_letters),
            "tasks": list(self.ledger.values()),
            "artifacts": self.blackboard.keys(),
        }, indent=2, default=str))
        print(f"\nartifacts written: {out}/report.md, {out}/ledger.json")
