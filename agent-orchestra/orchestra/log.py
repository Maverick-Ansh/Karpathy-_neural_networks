"""
log.py — OBSERVABILITY (the cheap, load-bearing kind).

THE CONCEPT
-----------
A concurrent system you can't see is a concurrent system you can't debug.
The log format here is deliberately structured and columnar:

    [  4.2s] researcher-3      START    research: origins and history — ...
    [  4.9s] researcher-3      DONE     0.7s ok
    [  5.0s] supervisor        SPAWN    analysis: analyse findings on ...
    [  6.1s] supervisor        RETRY    research ... attempt 2/3 in 0.9s (simulated 529)

    column 1: wall-clock seconds since the run started (so you can SEE
              interleaving and stage barriers)
    column 2: WHO (which agent / the supervisor)
    column 3: WHAT happened (the event verb)
    column 4: details

In production this would be structured JSON shipped to a log store, with the
task id as the *correlation id* tying every event of one task together. Here
it's pretty-printed for human eyes — but notice we ALSO write machine-readable
history to runs/<ts>/ledger.json. Logs for humans, ledgers for machines.
"""

from __future__ import annotations

import time

# ANSI escape codes — terminal colors with zero dependencies.
_COLORS = {
    "dim": "\033[2m", "red": "\033[31m", "green": "\033[32m",
    "yellow": "\033[33m", "blue": "\033[34m", "magenta": "\033[35m",
    "cyan": "\033[36m", "bold": "\033[1m", "reset": "\033[0m",
}

# One color per role so you can visually track an agent class through the noise.
ROLE_COLORS = {
    "planner": "magenta", "researcher": "cyan", "analyst": "blue",
    "critic": "yellow", "fact_checker": "green", "writer": "magenta",
    "editor": "magenta", "librarian": "dim", "supervisor": "bold",
    "orchestra": "bold",
}

VERB_COLORS = {
    "FAIL": "red", "DEAD": "red", "RETRY": "yellow", "DONE": "green",
    "PULSE": "dim", "PHASE": "magenta", "PILL": "dim",
}


class Console:
    def __init__(self, use_color: bool = True) -> None:
        self.t0 = time.monotonic()
        self.use_color = use_color

    def _c(self, name: str, text: str) -> str:
        if not self.use_color or not name:
            return text
        return f"{_COLORS.get(name, '')}{text}{_COLORS['reset']}"

    def line(self, actor: str, verb: str, msg: str) -> None:
        t = time.monotonic() - self.t0
        role = actor.rsplit("-", 1)[0]
        actor_s = self._c(ROLE_COLORS.get(role, ""), f"{actor:<16}")
        verb_s = self._c(VERB_COLORS.get(verb, ""), f"{verb:<7}")
        print(f"[{t:6.1f}s] {actor_s} {verb_s} {msg}", flush=True)
