#!/usr/bin/env python3
"""
main.py — the front door.

USAGE
-----
    # Mock mode (default): zero deps, zero cost, full orchestration mechanics
    python main.py
    python main.py --seed 7                       # reproducible run
    python main.py --failure-rate 0.5             # storm of retries + dead letters
    python main.py --concurrency 2                # strangle the semaphore, watch
                                                  # wall-time balloon
    python main.py --fanout 20                    # deeper backpressure

    # Real mode: pip install anthropic && export ANTHROPIC_API_KEY=sk-ant-...
    python main.py --real --mission "State of open-source LLMs in 2026"

WHY asyncio.run()?
------------------
Everything in this project is a coroutine — a function that can be paused at
`await` points. Coroutines don't run themselves; they need an EVENT LOOP that
juggles them: run one until it awaits, park it, run another, resume the first
when its awaited thing (queue item, timer, HTTP byte) is ready. asyncio.run()
creates that loop, runs our top-level coroutine to completion on it, and
tears it down. One thread, one loop, 25 agents + supervisor + heartbeat all
interleaved — concurrency without parallelism, which is exactly right for
workloads that spend their lives WAITING on the network (LLM calls).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from orchestra.config import ROLES, Settings, TOTAL_AGENTS
from orchestra.llm import AnthropicLLM, MockLLM
from orchestra.orchestrator import Orchestrator


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"Conduct an orchestra of {TOTAL_AGENTS} agents on a research mission.")
    d = Settings()  # defaults live in config.py, surfaced here
    p.add_argument("--mission", default=d.mission, help="what the orchestra should investigate")
    p.add_argument("--real", action="store_true",
                   help="use the Claude API (needs `pip install anthropic` + ANTHROPIC_API_KEY); "
                        "default is the free MockLLM")
    p.add_argument("--fanout", type=int, default=d.fanout, help="subtopics the planner targets")
    p.add_argument("--concurrency", type=int, default=d.max_concurrency,
                   help="max simultaneous LLM calls (the global semaphore)")
    p.add_argument("--seed", type=int, default=None, help="seed mock randomness (reproducible runs)")
    p.add_argument("--failure-rate", type=float, default=d.failure_rate,
                   help="mock-mode chance an LLM call fails (default 0.12; try 0.5)")
    p.add_argument("--no-pulse", action="store_true", help="disable the heartbeat lines")
    p.add_argument("--no-color", action="store_true", help="plain output (logs, CI)")
    return p.parse_args()


def build_settings(args: argparse.Namespace) -> Settings:
    return Settings(
        mission=args.mission,
        real=args.real,
        fanout=args.fanout,
        max_concurrency=args.concurrency,
        seed=args.seed,
        failure_rate=args.failure_rate,
        pulse_every=0 if args.no_pulse else Settings.pulse_every,
        color=(not args.no_color) and sys.stdout.isatty(),
        # Keep run outputs next to this script regardless of where you launch from.
        runs_dir=str(Path(__file__).resolve().parent / "runs"),
    )


def build_llm(settings: Settings):
    """Pick the brain. This is the ONLY place mock vs real is decided —
    everything downstream sees the same `complete()` interface (llm.py)."""
    if not settings.real:
        return MockLLM(seed=settings.seed, failure_rate=settings.failure_rate,
                       fanout=settings.fanout)
    if not os.environ.get("ANTHROPIC_API_KEY"):
        sys.exit("--real needs the ANTHROPIC_API_KEY environment variable "
                 "(get one at https://platform.claude.com). Or drop --real "
                 "and learn the machinery for free in mock mode.")
    try:
        return AnthropicLLM()
    except ImportError:
        sys.exit("--real needs the SDK: pip install anthropic")


def main() -> None:
    args = parse_args()
    settings = build_settings(args)

    print(f"agent-orchestra: {TOTAL_AGENTS} agents across {len(ROLES)} roles | "
          f"mode={'REAL (Claude API)' if settings.real else 'MOCK (free)'} | "
          f"concurrency={settings.max_concurrency} fanout={settings.fanout}")

    orchestrator = Orchestrator(llm=build_llm(settings), settings=settings)
    try:
        report = asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        # Ctrl-C: asyncio.run() cancels every pending task on the loop for us
        # (agents, supervisor, timers) before tearing the loop down.
        sys.exit("\ninterrupted — partial work is lost, queues evaporate with the process")

    print("\n" + "=" * 66)
    print(report)


if __name__ == "__main__":
    main()
