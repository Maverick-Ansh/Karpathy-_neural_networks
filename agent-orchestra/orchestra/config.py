"""
config.py — the CAST and the KNOBS.

THE CONCEPT
-----------
An "agent" here = a role (system prompt / persona) + a model tier + a loop.
The intelligence is rented from the LLM; what makes the *system* smart is the
DIVISION OF LABOUR — many narrow specialists beat one giant prompt because:

  1. Focus: a 5-line "you are a critic" prompt outperforms one 200-line
     do-everything prompt at criticising. Context is a budget; spend it on
     one job.
  2. Parallelism: 8 researchers work 12 subtopics simultaneously.
  3. Adversarial structure: critics and fact-checkers exist to DISAGREE with
     researchers. You design checks-and-balances into the org chart itself
     (this is why review steps are separate agents, not a paragraph appended
     to the researcher prompt — the same context that wrote a claim is the
     worst context to audit it).

THE ROSTER (25 agents — count them):

    1  planner      smart   decomposes the mission into subtopics
    8  researchers  fast    dig up findings, one subtopic each (fan-OUT)
    5  analysts     fast    turn findings into implications
    4  critics      fast    attack the analyses, find the gaps
    3  fact_checkers fast   audit claims in parallel with the analysts
    2  writers      smart   each writes a competing draft (best-of-N pattern)
    1  editor       smart   merges the drafts into the final report (fan-IN)
    1  librarian    fast    indexes every artifact at the end
   --
   25

Why 8 researchers for 12 subtopics? On purpose: 12 tasks > 8 workers means
4 tasks must QUEUE. You'll see `q[researcher]=4` in the heartbeat — that's
backpressure made visible, and it's the normal state of real systems
(capacity is provisioned for average load, queues absorb the peaks).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RoleSpec:
    count: int            # how many clones of this agent to hire
    tier: str             # "fast" or "smart" — which model serves it (llm.py)
    blurb: str            # one-liner for logs and the roster printout
    system: str           # the persona — this IS the agent, as far as the LLM knows


ROLES: dict[str, RoleSpec] = {
    "planner": RoleSpec(
        count=1, tier="smart",
        blurb="decomposes the mission into research subtopics",
        system=(
            "You are the planning agent of a research team. Given a mission, "
            "decompose it into sharp, non-overlapping research subtopics. "
            "Return ONLY a JSON array of short subtopic strings — no prose, "
            "no markdown fences, nothing else."
        ),
    ),
    "researcher": RoleSpec(
        count=8, tier="fast",
        blurb="digs up findings on one subtopic",
        system=(
            "You are a research agent. Investigate the assigned subtopic and "
            "return 3-5 numbered findings. Be concrete and specific; include "
            "one caveat about the limits of your evidence. Start your answer "
            "with 'FINDINGS — <topic>'."
        ),
    ),
    "analyst": RoleSpec(
        count=5, tier="fast",
        blurb="turns findings into patterns and implications",
        system=(
            "You are an analysis agent. Read the upstream findings and extract "
            "the underlying pattern and its implication for the mission. "
            "Do not repeat the findings; add interpretation. Start with "
            "'ANALYSIS — <topic>'."
        ),
    ),
    "critic": RoleSpec(
        count=4, tier="fast",
        blurb="attacks the analyses, hunts for gaps",
        system=(
            "You are a critic agent. Your job is to find what's WRONG or "
            "MISSING in the upstream analysis: unsupported leaps, missing "
            "counter-evidence, vague claims. One strength, one gap, one "
            "suggested follow-up. Start with 'CRITIQUE — <topic>'."
        ),
    ),
    "fact_checker": RoleSpec(
        count=3, tier="fast",
        blurb="audits claims independently of the analysts",
        system=(
            "You are a fact-checking agent. List the checkable claims in the "
            "upstream findings and flag any that look shaky or need a primary "
            "source. Start with 'FACT-CHECK — <topic>'."
        ),
    ),
    "writer": RoleSpec(
        count=2, tier="smart",
        blurb="writes a competing draft of the final report",
        system=(
            "You are a writing agent. Synthesize the supplied analyses and "
            "critiques into a coherent markdown report with a clear narrative. "
            "Another writer is drafting in parallel — make yours the better "
            "one. Start with '# DRAFT REPORT'."
        ),
    ),
    "editor": RoleSpec(
        count=1, tier="smart",
        blurb="merges competing drafts into the final report",
        system=(
            "You are the editor. You receive competing drafts of the same "
            "report. Merge them: keep the stronger structure, fold in the "
            "better passages of the other, cut repetition, tighten prose. "
            "Start with '# FINAL REPORT'."
        ),
    ),
    "librarian": RoleSpec(
        count=1, tier="fast",
        blurb="indexes every artifact produced during the run",
        system=(
            "You are the librarian. Produce a tidy index of the artifact keys "
            "you are given, grouped by pipeline stage. Start with 'INDEX'."
        ),
    ),
}

TOTAL_AGENTS = sum(spec.count for spec in ROLES.values())
assert TOTAL_AGENTS == 25, f"the orchestra hires exactly 25, got {TOTAL_AGENTS}"


# THE WORKFLOW (a.k.a. the "routine"): which stage spawns which next stage.
# This dict IS the pipeline. The supervisor consults it every time a task
# succeeds — event-driven chaining, not a hardcoded script. Note the fan-out:
# one research success spawns TWO children (analysis + factcheck) that run in
# parallel on different roles.
#
#   plan ──▶ research ──┬──▶ analysis ──▶ critique        (the pipeline phase)
#                       └──▶ factcheck
#   ...then, once the pipeline drains:
#   draft ×2 (writers compete) ──▶ edit ──▶ archive ──▶ done   (the finale)
#
WORKFLOW: dict[str, list[tuple[str, str]]] = {
    # stage      -> [(child_stage, child_role), ...]
    "plan":      [],            # handled specially: its output BECOMES the research tasks
    "research":  [("analysis", "analyst"), ("factcheck", "fact_checker")],
    "analysis":  [("critique", "critic")],
    "critique":  [],            # leaf — the chain ends here
    "factcheck": [],            # leaf
    "draft":     [],            # finale stages are sequenced by the supervisor's
    "edit":      [],            # phase machine, not by this table
    "archive":   [],
}

# Task priorities (lower = more urgent). The plan unblocks everything, so it
# goes first; the finale stages jump any stragglers; bulk work sits at 3-5.
PRIORITY = {"plan": 0, "draft": 1, "edit": 1, "archive": 2,
            "research": 3, "analysis": 4, "factcheck": 4, "critique": 5}


@dataclass
class Settings:
    """Every tuning knob in one place. The interesting ones:

    max_concurrency — the GLOBAL cap on simultaneous LLM calls (a semaphore,
        see agent.py). 25 agents, but at most 8 in-flight calls: this is how
        you respect API rate limits and is itself another queue — agents 9+
        wait at the semaphore. Watch wall-time change when you tune it.
    fanout — how many subtopics the planner aims for. fanout > researcher
        count (12 > 8) is deliberate; see the backpressure note above.
    max_attempts / retry_base — task-level retry policy. Delay grows
        retry_base * 2^attempt plus jitter (orchestrator._requeue_later).
    failure_rate — mock-mode chaos. Try 0.5 and watch dead letters appear.
    task_timeout — NEVER await an LLM (or any network call) without a
        timeout. A hung call would otherwise hold its agent + semaphore slot
        hostage forever.
    """

    mission: str = ("How do neural networks learn? From backpropagation "
                    "to modern large language models.")
    real: bool = False                # False = MockLLM, True = Claude API
    fanout: int = 12
    max_concurrency: int = 8
    max_attempts: int = 3
    retry_base: float = 0.5
    task_timeout: float = 120.0
    failure_rate: float = 0.12        # mock mode only
    seed: int | None = None           # mock mode only; set for reproducible runs
    pulse_every: float = 2.0          # heartbeat period; 0 disables
    ctx_snippet_chars: int = 1500     # max chars of each upstream artifact we
                                      # forward — context windows are a budget,
                                      # and unbounded forwarding is how agent
                                      # pipelines blow up in production
    color: bool = True
    runs_dir: str = "runs"
