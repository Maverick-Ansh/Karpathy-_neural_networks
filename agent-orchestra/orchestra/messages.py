"""
messages.py — the PROTOCOL layer.

THE CONCEPT
-----------
In a multi-agent system, agents never call each other's functions directly.
They exchange *messages* through *queues*. This is the single most important
design decision in the whole codebase, and it's the same decision made by
Erlang, Akka, Celery, Kafka consumers, and every serious distributed system:

    shared mutable state  -->  hard to reason about, race-prone, tightly coupled
    message passing       -->  each agent is an island; you can kill, retry,
                               or replace any of them without touching the rest

Two message types flow through the system:

    Task    (work to do)        orchestrator ──▶ agent
    Result  (what happened)     agent ──▶ orchestrator

That's it. Agents never see each other. The orchestrator is the postal service.
"""

from __future__ import annotations

import asyncio
import itertools
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    """The task LIFECYCLE — a tiny state machine.

    PENDING ──▶ RUNNING ──▶ DONE
                   │
                   ├──▶ (failed, attempts left) ──▶ PENDING again (retry!)
                   └──▶ (failed, out of attempts) ──▶ DEAD  (the "dead letter")

    Real-world queue systems (SQS, RabbitMQ, Celery) have exactly this shape:
    visible/in-flight/acked/dead-lettered. Same idea, smaller words.
    """

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    DEAD = "dead"          # exhausted all retries — parked in the dead-letter list


@dataclass
class Task:
    """A unit of work. Deliberately tiny and SELF-CONTAINED.

    A task carries everything the agent needs in `context` (the mission, the
    topic, upstream artifacts). The agent should not need to go read shared
    state to do its job — that property is called *location transparency*:
    you could ship this Task over a network to a worker on another machine
    and it would still work. That's how you'd scale this beyond one process.
    """

    role: str                      # which KIND of agent should handle this ("researcher", ...)
    stage: str                     # which pipeline stage this belongs to ("research", "critique", ...)
    description: str               # human/LLM-readable statement of the work
    priority: int = 5              # lower number = more urgent (0 is highest)
    context: dict = field(default_factory=dict)   # mission, topic, upstream artifacts
    parent_id: str | None = None   # which task spawned this one (forms a tree — the "task DAG")
    attempts: int = 0              # how many times we've tried it (retries increment this)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    created_at: float = field(default_factory=time.time)


@dataclass
class Result:
    """What an agent sends back after attempting a Task.

    NOTE: an agent ALWAYS produces exactly one Result per Task, success or
    failure. The agent never raises, never dies, never goes silent. The
    supervisor's bookkeeping (open-task counting) depends on this invariant —
    one Task in, one Result out. If an agent could crash without reporting,
    the orchestrator would wait forever. This is why agent.py wraps the whole
    work step in try/except.
    """

    task: Task
    ok: bool
    output: str = ""               # the artifact produced (text), if ok
    error: str = ""                # what went wrong, if not ok
    agent_name: str = ""           # who did the work (for the ledger / log)
    elapsed: float = 0.0           # seconds spent thinking (LLM latency)


# A unique sentinel object. When an agent pulls this from its inbox it shuts
# down cleanly. This is the classic "poison pill" pattern: to stop N workers
# sharing one queue, you enqueue N pills. Each pill kills exactly one worker,
# and because we enqueue pills at the LOWEST priority, workers always finish
# real work first. No flags, no signals, no force-kill — just one more message.
POISON = object()


class Mailbox:
    """A priority queue of tasks — the in-tray for one ROLE (shared by all
    agents of that role).

    WHY A QUEUE AT ALL? Queues *decouple* producers from consumers:
      - the orchestrator can enqueue 12 research tasks instantly even though
        only 8 researchers exist — the extra 4 simply WAIT. That waiting is
        called BACKPRESSURE, and you can watch it live in the heartbeat log
        (`q[researcher]=4` means four tasks are queued behind busy agents).
      - producers don't know or care which agent picks a task up. Add 10 more
        researchers and throughput rises with zero code changes elsewhere.

    WHY A *PRIORITY* QUEUE? So urgent work (the plan, the final synthesis)
    jumps ahead of bulk work, and so poison pills (priority 999) drain LAST.

    IMPLEMENTATION DETAIL worth knowing: asyncio.PriorityQueue compares the
    tuples you put in it. Tasks aren't comparable, so we insert
    (priority, sequence_number, item). The sequence number is unique and
    strictly increasing, so ties on priority are broken by insertion order
    (FIFO within a priority class) and Python never has to compare two items.
    """

    def __init__(self) -> None:
        self._q: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = itertools.count()      # global tie-breaker; see docstring

    def put_nowait(self, item, priority: int = 5) -> None:
        self._q.put_nowait((priority, next(self._seq), item))

    async def get(self):
        """Blocks (cooperatively!) until something is available.

        'Blocks' in asyncio-land means: this coroutine is parked and the event
        loop runs OTHER coroutines meanwhile. 25 agents awaiting 8 mailboxes
        costs essentially nothing — no threads, no spinning, no polling.
        """
        _prio, _seq, item = await self._q.get()
        return item

    def qsize(self) -> int:
        return self._q.qsize()
