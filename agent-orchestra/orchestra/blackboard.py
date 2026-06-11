"""
blackboard.py — SHARED MEMORY (used sparingly, on purpose).

THE CONCEPT
-----------
The "blackboard" is a classic AI architecture from the 1970s (Hearsay-II
speech recognition): many specialists look at one shared board, each writes
what it figured out, others build on it.

Ours is just a dict of  key -> text artifact, e.g.:

    research/origins-and-history-of-neural-networks   -> "FINDINGS — ..."
    analysis/origins-and-history-of-neural-networks   -> "ANALYSIS — ..."
    draft/draft-1                                      -> "REPORT ..."

DESIGN CHOICE — single writer:
Only the SUPERVISOR writes to the blackboard (when it processes a Result).
Agents never touch it; everything an agent needs arrives inside its Task.
This buys us:
  1. zero locking headaches,
  2. agents that would work unchanged across a network boundary,
  3. one place to audit every artifact the system produced.

A NOTE ON LOCKS (since you're here to learn the guts):
asyncio is single-threaded *cooperative* multitasking. Code only yields
control at an `await`. A plain `dict[key] = value` contains no await, so it
can never be interrupted halfway — it is atomic "for free". You only need an
asyncio.Lock when a read-modify-write sequence spans an await, e.g.:

    counter = board.get("n")        # read
    await something()               # <-- another coroutine may run HERE
    board.put("n", counter + 1)     # write a now-stale value. BUG.

We avoid the whole class of problem by having one writer. Simplest correct
design wins.
"""

from __future__ import annotations


class Blackboard:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def put(self, key: str, text: str) -> None:
        self._store[key] = text

    def get(self, key: str, default: str = "") -> str:
        return self._store.get(key, default)

    def keys(self, prefix: str = "") -> list[str]:
        """All artifact keys under a stage prefix, e.g. keys('analysis/')."""
        return sorted(k for k in self._store if k.startswith(prefix))

    def collect(self, prefix: str) -> dict[str, str]:
        """Grab every artifact under a prefix — used to build the synthesis
        context (the fan-IN step that mirrors the earlier fan-OUT)."""
        return {k: self._store[k] for k in self.keys(prefix)}

    def __len__(self) -> int:
        return len(self._store)
