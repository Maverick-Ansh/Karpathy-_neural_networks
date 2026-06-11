"""
llm.py — the BRAIN SOCKET.

THE CONCEPT
-----------
Every agent "thinks" by calling exactly one function:

    text = await llm.complete(system=..., prompt=..., tier=..., meta=...)

That one-line interface is the SEAM of the whole system. Everything above it
(agents, queues, supervisor) doesn't know or care what's behind it. Behind it
we ship two implementations:

    MockLLM      — fake brain. Random latency, canned-but-derived answers,
                   injected failures. Costs $0, needs no network. This is how
                   you study the ORCHESTRATION without paying for INTELLIGENCE.
                   (Distributed-systems folks call this a "simulator" or
                   "fault-injection harness" — Netflix's Chaos Monkey is the
                   famous grown-up version.)

    AnthropicLLM — real brain. Calls the Claude API with the official SDK.

Because the seam is one class, swapping in ANY other brain — an open-source
model served by Ollama or vLLM, some other provider — means writing one more
~30-line class. The README's "Open-source models" section shows exactly how.

TWO LAYERS OF RETRY (don't confuse them!)
-----------------------------------------
  transport retries — a single LLM call hiccuped (HTTP 429/500/529). The
                      Anthropic SDK retries these itself with exponential
                      backoff (we configure max_retries=4).
  task retries      — the call *ultimately* failed, or timed out, or the
                      agent choked. The SUPERVISOR re-enqueues the whole task
                      with backoff (see orchestrator.py). MockLLM's injected
                      failures exercise THIS layer, so you can watch it work.
"""

from __future__ import annotations

import asyncio
import json
import random


class TransientLLMError(Exception):
    """A failure worth retrying (overloaded API, timeout, flaky network)."""


# ---------------------------------------------------------------------------
# MockLLM — the $0 brain (and chaos monkey)
# ---------------------------------------------------------------------------

# The mock planner builds subtopics by combining the mission with these angles.
_ANGLES = [
    "origins and history",
    "core mechanisms and how it actually works",
    "key breakthroughs and landmark papers",
    "training data and learning dynamics",
    "hardware, compute and scaling",
    "limitations and failure modes",
    "evaluation, benchmarks and measurement",
    "open problems and active debates",
    "safety, alignment and societal impact",
    "the people and labs driving the field",
    "practical applications today",
    "plausible future directions",
]

_RESEARCH_VERBS = ["traced", "uncovered", "catalogued", "mapped", "surveyed"]
_PATTERNS = [
    "a recurring tension between scale and interpretability",
    "rapid progress driven by a small number of compounding ideas",
    "a gap between benchmark performance and real-world behaviour",
    "strong path-dependence on early architectural choices",
    "an arms race between capability gains and tooling maturity",
]
_GAPS = [
    "the evidence leans on secondary sources",
    "the timeline skips the 1990s 'AI winter' context",
    "no quantitative comparison is offered",
    "counter-examples from adjacent fields are ignored",
    "the causal story is asserted rather than demonstrated",
]


class MockLLM:
    """Pretends to be an LLM. Three behaviours make it a great teacher:

    1. LATENCY — every call sleeps a random 0.15-1.2s. This is what makes the
       25 agents visibly interleave; with zero latency, everything would
       finish in one burst and you'd learn nothing about concurrency.
    2. FAILURE INJECTION — `failure_rate` of calls raise TransientLLMError,
       simulating an overloaded API (HTTP 529). This is what lets you WATCH
       the supervisor's retry/backoff/dead-letter machinery actually fire.
    3. DERIVED OUTPUT — answers are templated from the *actual inputs*
       (topic + upstream artifacts), so data genuinely flows research →
       analysis → critique → report. It's fake intelligence but real plumbing.

    `seed` makes a run reproducible (same latencies, same failures) — gold
    when you're debugging concurrent systems, where "it only breaks sometimes"
    is the default state of the universe.
    """

    def __init__(self, seed: int | None = None, failure_rate: float = 0.12,
                 fanout: int = 12):
        self.rng = random.Random(seed)
        self.failure_rate = failure_rate
        self.fanout = fanout
        self.calls = 0
        self.injected_failures = 0

    async def complete(self, *, system: str, prompt: str, tier: str, meta: dict) -> str:
        self.calls += 1

        # Simulate inference latency. "smart" models are slower — you'll see
        # this show up in the per-role timing stats at the end of a run.
        lo, hi = (0.4, 1.2) if tier == "smart" else (0.15, 0.8)
        await asyncio.sleep(self.rng.uniform(lo, hi))

        # Chaos monkey: sometimes the "API" just falls over. The agent will
        # catch this, report a failed Result, and the supervisor will retry.
        if self.rng.random() < self.failure_rate:
            self.injected_failures += 1
            raise TransientLLMError("simulated 529: overloaded_error")

        stage = meta.get("stage", "")
        topic = meta.get("topic", "the subject")
        mission = meta.get("mission", "")
        artifacts: dict[str, str] = meta.get("artifacts", {})

        return self._generate(stage, topic, mission, artifacts)

    # -- canned-but-derived generators, one per pipeline stage ---------------

    def _generate(self, stage: str, topic: str, mission: str,
                  artifacts: dict[str, str]) -> str:
        if stage == "plan":
            # The planner must output machine-readable JSON — exactly what
            # we'd ask a real model for. Downstream code parses defensively
            # anyway (see orchestrator._parse_plan), because real LLMs are
            # probabilistic and WILL eventually hand you malformed output.
            return json.dumps(_ANGLES[: self.fanout])

        if stage == "research":
            verb = self.rng.choice(_RESEARCH_VERBS)
            return (
                f"FINDINGS — {topic}\n"
                f"1. We {verb} three primary threads, the strongest being "
                f"{self.rng.choice(_PATTERNS)}.\n"
                f"2. Key datapoint: roughly {self.rng.randint(60, 95)}% of the "
                f"sources agree on the central claim.\n"
                f"3. Caveat for downstream analysts: {self.rng.choice(_GAPS)}."
            )

        if stage == "analysis":
            # Quote an actual finding from the upstream artifact — proof in
            # the final report that data really flowed through the pipeline.
            upstream = self._first_body(artifacts) or "(no upstream findings)"
            return (
                f"ANALYSIS — {topic}\n"
                f"Building on: \"{upstream}\"\n"
                f"Pattern: {self.rng.choice(_PATTERNS)}.\n"
                f"Implication: this matters for '{mission}' because it changes "
                f"what we should measure next."
            )

        if stage == "factcheck":
            n = self.rng.randint(3, 6)
            return (
                f"FACT-CHECK — {topic}\n"
                f"{n} claims reviewed; {n - 1} look solid. "
                f"Flagged: {self.rng.choice(_GAPS)}."
            )

        if stage == "critique":
            return (
                f"CRITIQUE — {topic}\n"
                f"Strength: the analysis connects evidence to implication.\n"
                f"Gap: {self.rng.choice(_GAPS)}.\n"
                f"Suggested follow-up: a focused source-comparison pass."
            )

        if stage == "draft":
            # The writer fans IN: one line per analysed subtopic. This is
            # where you can verify data really crossed the whole pipeline —
            # these bullets are derived from the analysts' actual outputs.
            analyses = {k: v for k, v in artifacts.items() if k.startswith("analysis/")}
            n_critiques = sum(1 for k in artifacts if k.startswith("critique/"))
            lines = [
                f"- **{k.split('/', 1)[1]}**: {self._first_body_line(v)}"
                for k, v in sorted(analyses.items())
            ] or ["- (no analyses survived the pipeline)"]
            style = self.rng.choice(["measured and survey-like", "punchy and opinionated"])
            return (
                f"# DRAFT REPORT: {mission}\n\n"
                f"_Style: {style}._\n\n"
                f"## What we learned\n" + "\n".join(lines) + "\n\n"
                f"## Bottom line\nAcross {len(analyses)} analysed threads "
                f"(stress-tested by {n_critiques} critiques), the dominant theme "
                f"is {self.rng.choice(_PATTERNS)}."
            )

        if stage == "edit":
            drafts = list(artifacts.values())
            base = drafts[0] if drafts else "# REPORT\n(no drafts arrived)"
            note = (
                f"\n\n---\n_Editor's note: merged {len(drafts)} competing drafts; "
                f"kept the stronger structure, folded in the alternate framing._"
            )
            return base.replace("DRAFT REPORT", "FINAL REPORT") + note

        if stage == "archive":
            listing = artifacts.get("__keys__", "")
            return (
                f"INDEX\nCatalogued artifacts for posterity:\n{listing}"
                if listing else "INDEX\n(nothing to archive)"
            )

        return f"OK — handled '{stage}' for {topic}."

    @classmethod
    def _first_body(cls, artifacts: dict[str, str]) -> str:
        for text in artifacts.values():
            return cls._first_body_line(text)
        return ""

    @staticmethod
    def _first_body_line(text: str) -> str:
        """First informative line after the 'HEADER — topic' line."""
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        return lines[1] if len(lines) > 1 else (lines[0] if lines else "")


# ---------------------------------------------------------------------------
# AnthropicLLM — the real brain
# ---------------------------------------------------------------------------

# Model "tiers": which Claude model serves which class of work. Both tiers
# default to Opus 4.8 (the recommended general-purpose model). If you want to
# trade quality for cost/speed on the high-volume worker tier — a classic
# production lever called MODEL TIERING — that's YOUR call to make, e.g.:
#
#     "fast": "claude-haiku-4-5"     # $1 / $5 per Mtok  — cheap bulk labour
#     "fast": "claude-sonnet-4-6"    # $3 / $15 per Mtok — balanced
#
# Prices as of 2026-06 (per million tokens, input/output) — check
# https://platform.claude.com/docs/en/pricing before believing a comment.
TIER_MODELS = {
    "fast": "claude-opus-4-8",     # the 19 worker bees (researchers, critics, ...)
    "smart": "claude-opus-4-8",    # planner, writers, editor ($5 / $25 per Mtok)
}

_PRICES_PER_MTOK = {  # (input $, output $) — for the end-of-run cost estimate
    "claude-opus-4-8": (5.00, 25.00),
    "claude-sonnet-4-6": (3.00, 15.00),
    "claude-haiku-4-5": (1.00, 5.00),
}


class AnthropicLLM:
    """Real Claude calls via the official async SDK.

    Things to notice:
    - AsyncAnthropic: the async client. Our whole system is one event loop;
      a blocking (sync) HTTP call would freeze ALL 25 agents for its duration.
    - max_retries=4: the SDK transparently retries 429/5xx with exponential
      backoff — that's the TRANSPORT retry layer (see module docstring).
    - The API key is read from the ANTHROPIC_API_KEY environment variable.
      Never hardcode keys; never commit them.
    - thinking={"type": "adaptive"}: lets the model decide when to reason
      before answering (recommended setting for the 4.7+ models).
    - We sum token usage per model so the run can print an honest cost line.
    """

    def __init__(self) -> None:
        # Imported lazily so mock mode runs with zero third-party deps.
        from anthropic import AsyncAnthropic

        self.client = AsyncAnthropic(max_retries=4)
        self.calls = 0
        self.usage: dict[str, dict[str, int]] = {}   # model -> {input, output}

    async def complete(self, *, system: str, prompt: str, tier: str, meta: dict) -> str:
        import anthropic

        model = TIER_MODELS[tier]
        self.calls += 1
        try:
            msg = await self.client.messages.create(
                model=model,
                max_tokens=16000,
                thinking={"type": "adaptive"},
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
        except (anthropic.RateLimitError, anthropic.InternalServerError,
                anthropic.APIConnectionError) as e:
            # SDK retries are exhausted by the time we get here; hand the
            # failure up so the SUPERVISOR can do a task-level retry later.
            raise TransientLLMError(f"{type(e).__name__}: {e}") from e

        u = self.usage.setdefault(model, {"input": 0, "output": 0})
        u["input"] += msg.usage.input_tokens
        u["output"] += msg.usage.output_tokens

        # Check WHY generation stopped before trusting the content. A refusal
        # returns HTTP 200 with empty/partial content — naive code that grabs
        # content[0] would crash or, worse, ship garbage downstream.
        if msg.stop_reason == "refusal":
            raise TransientLLMError("model refused this request")

        # Content is a list of typed blocks (thinking, text, ...). Take text.
        return "".join(b.text for b in msg.content if b.type == "text")

    def cost_estimate(self) -> tuple[float, str]:
        total, parts = 0.0, []
        for model, u in self.usage.items():
            pin, pout = _PRICES_PER_MTOK.get(model, (0, 0))
            cost = u["input"] / 1e6 * pin + u["output"] / 1e6 * pout
            total += cost
            parts.append(f"{model}: {u['input']:,} in / {u['output']:,} out ≈ ${cost:.4f}")
        return total, "; ".join(parts) or "no calls"
