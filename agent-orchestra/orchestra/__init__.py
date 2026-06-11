"""
agent-orchestra — a 25-agent orchestration lab, built for learning.

Read the files in this order (each one teaches one layer of the stack):

    1. messages.py      — the "protocol": Task / Result objects + the Mailbox (queue)
    2. blackboard.py    — shared memory, and why we barely use it
    3. llm.py           — the brain socket: MockLLM (free) and AnthropicLLM (real)
    4. config.py        — the cast of 25 agents and the tuning knobs
    5. agent.py         — THE AGENT LOOP (the heartbeat of every agent)
    6. orchestrator.py  — the conductor: routing, supervision, retries, phases
    7. ../main.py       — CLI entry point

The README.md next to main.py is the textbook chapter that ties it together.
"""

__version__ = "0.1.0"
