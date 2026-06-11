# agent-orchestra 🎻

**25 LLM agents, one conductor, zero magic.** A from-scratch multi-agent
orchestration system in ~900 lines of annotated, dependency-free Python —
built to be read, broken, and rebuilt. The code is the textbook; this README
is the lecture.

```bash
# That's it. No API key, no pip install, nothing. Watch 25 agents work:
cd agent-orchestra
python3 main.py
```

By default the agents run on a **MockLLM** — fake brain, real plumbing: real
queues, real concurrency, real retries, simulated latency and simulated API
failures. You learn 95% of multi-agent engineering (the orchestration) for
$0, then flip one flag (`--real`) to rent actual intelligence.

---

## 1. What an "agent" actually is

Strip the hype and an agent is **a loop around a brain**:

```python
while True:
    task   = await inbox.get()        # PERCEIVE — wait for work
    result = await think(task)        # DECIDE   — the LLM call
    await outbox.put(result)          # ACT      — report back
```

That's `orchestra/agent.py`, and it's the whole secret. Claude Code, AutoGPT,
every swarm framework — under the costume, each agent is some elaboration of
those three lines. What makes a *system* of agents interesting isn't the loop;
it's everything around it: **who decides what to work on, how work travels,
what happens when things fail, and how you know you're done.** That's
orchestration, and that's what this repo teaches.

## 2. The cast (count them — 25)

| # | role | model tier | job |
|---|------|-----------|-----|
| 1 | `planner` | smart | decomposes the mission into ~12 subtopics |
| 8 | `researcher` | fast | dig up findings, one subtopic each *(fan-out)* |
| 5 | `analyst` | fast | turn findings into patterns & implications |
| 4 | `critic` | fast | attack the analyses, hunt for gaps |
| 3 | `fact_checker` | fast | audit claims, in parallel with the analysts |
| 2 | `writer` | smart | each writes a *competing* draft *(best-of-N)* |
| 1 | `editor` | smart | merges the drafts into the final report *(fan-in)* |
| 1 | `librarian` | fast | indexes every artifact at the end |

Why specialists instead of one giant prompt? Three reasons (see
`config.py`):

1. **Focus** — a 5-line "you are a critic" persona out-criticises a 200-line
   do-everything prompt. Context is a budget; spend it on one job.
2. **Parallelism** — 8 researchers chew 12 subtopics simultaneously.
3. **Adversarial structure** — critics and fact-checkers exist to *disagree*
   with researchers. The same context that wrote a claim is the worst context
   to audit it, so checks-and-balances are built into the org chart, not
   bolted onto a prompt.

## 3. The architecture, on one screen

```
                                   ┌──────────────────────────────────────────┐
                                   │              ORCHESTRATOR                │
            mission                │                                          │
               │                   │  submit() ── routes Task by role ──┐     │
               ▼                   │     ▲                              │     │
        ┌────────────┐             │     │ spawns children              │     │
        │ SUPERVISOR │◀── Result ──┤  WORKFLOW table                    │     │
        │   LOOP     │   queue     │  retry / dead-letter policy        │     │
        └────────────┘             │  phase machine + termination count │     │
               │                   └────────────────────────────────────┼─────┘
        writes artifacts                                                │ Task
               ▼                                                        ▼
        ┌────────────┐         ┌───────────────────── role Mailboxes ─────────┐
        │ BLACKBOARD │         │ q[planner] q[researcher] q[analyst] q[critic]│
        │ (shared    │         │ q[fact_checker] q[writer] q[editor] q[libr.] │
        │  memory)   │         └───────┬───────────┬───────────┬──────────────┘
        └────────────┘                 ▼           ▼           ▼
                              25 AGENT LOOPS (competing consumers per role)
                                       │   all LLM calls squeeze through
                                       ▼   one global Semaphore(8)
                                  ┌─────────┐
                                  │   LLM   │  MockLLM ($0)  or  Claude API
                                  └─────────┘
```

Everything flows **through the middle** (a star topology). Agents never talk
to each other — peer-to-peer agent meshes look cool in diagrams and are
miserable to debug. The supervisor is the single consumer of results and the
single writer of shared state: the system's *brain* is single-threaded even
though its *hands* are concurrent. (That's the actor-model insight, and it's
why this codebase has zero locks.)

And the work itself forms a pipeline, chained event-by-event:

```
plan ──▶ research ──┬──▶ analysis ──▶ critique     }  the PIPELINE phase
      (fan-out ×12) └──▶ factcheck                 }  (stages overlap freely)
─── pipeline drains (open_tasks == 0) ─────────────── the fan-IN barrier
draft ×2 (writers compete) ──▶ edit ──▶ archive ──▶ DONE
```

## 4. The agent culture: loops, routines, queues

This is the vocabulary you asked for — the "culture" every agent system is
built from. Each concept names a file where you can see it implemented.

### 4.1 The event loop (`main.py`)

All 27 loops (25 agents + supervisor + heartbeat) run on **one thread**,
juggled by Python's asyncio event loop. A coroutine runs until it hits
`await` (queue empty, timer, network byte pending), gets parked, and someone
else runs. LLM work is ~99% *waiting on the network*, which is why async —
not threads, not processes — is the right tool. The end-of-run stats print
the payoff: ~36s of combined agent think-time squeezed into ~7s of wall
clock (5.2× on the default run).

### 4.2 Queues (`messages.py`)

Queues **decouple** producers from consumers — the load-bearing idea of all
distributed systems:

- The planner can fan out 12 tasks instantly even though only 8 researchers
  exist. The extra 4 *wait in the queue*. That waiting is **backpressure**,
  and the heartbeat makes it visible:

  ```
  [   2.0s] supervisor   PULSE   open=19 done=21 retries=2 dead=0 q[critic]=1 q[fact_checker]=4
  ```

  `q[fact_checker]=4` — four tasks queued behind 3 busy fact-checkers. That's
  a healthy system absorbing a burst, the normal state of production
  infrastructure.
- Agents of one role share one mailbox and whoever's free grabs the next task
  — the **competing consumers** pattern. Hire 10 more researchers and
  throughput rises with zero code changes anywhere else.
- Ours are **priority queues**: the plan and the finale jump ahead of bulk
  work; poison pills (priority 999) sort behind everything.

### 4.3 The agent loop (`agent.py`) — and its three rules

1. **Never die** — every exception becomes a *failed Result*. A worker that
   crashes silently strands its queue and deadlocks the run (the supervisor's
   bookkeeping depends on exactly one Result per Task).
2. **Never hog** — a global `Semaphore(8)` caps simultaneous LLM calls
   (that's your API rate-limit budget), and every call has a timeout. Never
   await a network call without a timeout.
3. **Never decide** — workers don't retry, don't route, don't spawn. All
   *policy* lives in the supervisor. Dumb workers + smart coordinator =
   a system you can reason about (Erlang calls this a *supervision tree*).

### 4.4 The supervisor loop (`orchestrator.py`)

One loop consumes every Result and makes every decision:

- **success** → archive the artifact on the blackboard, consult the
  `WORKFLOW` table, spawn children. This is **event-driven chaining**: nobody
  scripts "now run all analyses" — each research success independently
  triggers its own analysis, so stages overlap naturally.
- **failure** → retry with **exponential backoff + jitter** (0.5s, 1s, 2s…
  ×random 0.5–1.5). Backoff gives a struggling dependency room to recover;
  jitter prevents the *thundering herd* where all failures retry in lockstep
  and knock the service over again.
- **out of retries** → the **dead letter** list. A poison task that always
  fails must not loop forever; park it for the post-mortem. The pipeline
  *degrades gracefully* — that branch is pruned, downstream stages just see
  fewer artifacts, and you still get a (thinner) report.

### 4.5 Termination — how do you know a swarm is *done*?

Queues-empty is **not** enough: an agent might be mid-task, about to spawn
three children into those empty queues. The standard trick is counting:

```
open_tasks += 1  on submit
open_tasks -= 1  when a task goes TERMINAL (DONE or DEAD — a retry is NOT terminal)
open_tasks == 0  ⇒ the phase has fully drained ⇒ advance the phase machine
```

Because a task awaiting retry stays counted, the phase can never advance
while a retry timer is pending — the invariant is race-free by construction.
(The alternative for single queues is `Queue.join()`/`task_done()`; counting
generalises it to a graph of queues that spawn into each other.)

### 4.6 Routines (background coroutines)

The **heartbeat** prints queue depths and counters every 2s — a periodic
routine alongside the work. Real orchestrators run several: metrics flushers,
watchdogs (kill tasks running too long), autoscalers (queue deep? hire more
workers). Try writing the watchdog — it's exercise #7.

### 4.7 Shutdown — poison pills

To stop N workers sharing a queue, enqueue N **poison pills** (a sentinel
object). Each pill stops exactly one worker, after all real work has drained.
No flags, no signals, no force-kill — just one more message. Watch the
`PILL shutting down` lines at the end of every run.

### 4.8 The task lifecycle (state machine)

```
PENDING ──▶ RUNNING ──▶ DONE
               │
               ├──(failed, attempts left)──▶ PENDING   (retry, with backoff)
               └──(failed, out of attempts)─▶ DEAD     (dead letter)
```

Every transition of every task is recorded in `runs/<ts>/ledger.json` —
**logs are for humans, ledgers are for machines.** Open one after a run and
trace a task id through the console log; that id is your *correlation id*.

## 5. The patterns dictionary

Things you saw above, with their industry names — these compose into every
agent system you'll ever meet:

| pattern | here | grown-up version |
|---|---|---|
| fan-out / fan-in | plan → 12 researches; drain → drafts | MapReduce |
| pipeline | research → analysis → critique | ETL, compiler passes |
| competing consumers | 8 researchers, 1 mailbox | Celery/SQS workers |
| best-of-N + judge | 2 writers, 1 editor | sampling + reranking |
| supervisor + dead letters | retry/backoff/DEAD | Erlang/OTP, RabbitMQ DLQ |
| poison pill | shutdown | classic threadpool idiom |
| blackboard | shared artifact store | Hearsay-II (1970s!), shared KV |
| backpressure | bounded workers + queues | TCP windows, Reactive Streams |
| model tiering | fast/smart tiers | prod cost engineering |
| chaos injection | `--failure-rate` | Netflix Chaos Monkey |

## 6. Learn by breaking it (do these, in order)

1. `python3 main.py --seed 7` twice — identical runs. **Reproducibility**:
   seeded randomness turns "it only breaks sometimes" into "it breaks at
   t=1.7s, every time". Debugging concurrency without it is astrology.
2. `--failure-rate 0.5` — a retry storm. Watch `RETRY` lines, then your first
   `DEAD` letters, then note the run *still finishes* with a thinner report.
3. `--failure-rate 0.97` — total API outage. The plan dead-letters and the
   mission aborts cleanly instead of hanging. (Hanging-instead-of-failing is
   the default behaviour of naive async code — see exercise 6.)
4. `--concurrency 2` vs `--concurrency 25` — strangle and un-strangle the
   semaphore; compare wall time and the "concurrency payoff" line.
5. `--fanout 20` — deeper backpressure; watch `q[researcher]` climb.
6. **Sabotage**: in `agent.py`, delete the `try/except` and rerun with
   failures on. The run hangs forever — you've just proven rule #1 (never
   die) is load-bearing, because a missing Result breaks the open-task count.
7. **Build**: add a watchdog routine that warns when any task has been
   RUNNING longer than 10s. (Hint: copy `heartbeat()`, scan the ledger.)
8. **Build**: add a `translator` role and wire a new stage into `WORKFLOW`
   so every critique gets translated. Notice you never touch agent.py — the
   org chart is data, not code.
9. **Think**: why does the editor see *both* drafts but a critic sees only
   *its own* analysis? (Answer: fan-in vs pipeline; context is a budget.)
10. Open `runs/<ts>/ledger.json`, find a task with `attempts: 2`, and trace
    its id through the console log start-to-finish.

## 7. Running it on real intelligence (Claude API)

**What you need:**

1. Python 3.10+ (you already have it if mock mode ran)
2. `pip install anthropic`
3. An API key from https://platform.claude.com → `export ANTHROPIC_API_KEY=sk-ant-...`

```bash
python3 main.py --real --fanout 6 --mission "The state of open-source LLMs in 2026"
```

What changes in the code? **One constructor** (`build_llm()` in `main.py`).
Everything else — queues, retries, phases, agents — is identical, because the
LLM sits behind a one-method interface (`llm.py`). That seam is the most
important line in the repo.

Real-mode notes (all annotated in `llm.py`):

- **Two retry layers**: the Anthropic SDK transparently retries transient
  HTTP failures (429/5xx) per call; our supervisor retries whole *tasks*.
  Transport retries vs task retries — different layers, different jobs.
- **Model tiering**: both tiers default to `claude-opus-4-8`. The
  `TIER_MODELS` dict is where you'd point the 19 "fast"-tier workers at a
  cheaper model (`claude-haiku-4-5` is ~5× cheaper per token) and keep the
  planner/writers/editor on the big model — the classic production cost
  lever. Your trade-off to make.
- **Cost**: the run prints token usage and an estimated bill at the end.
  Start with `--fanout 4` and small missions; 53 tasks × real prompts adds up.
- The semaphore (`--concurrency`) is now your rate-limit governor for real.

## 8. Swapping in an open-source model (no code changes shipped, by design)

The `llm.py` seam means *any* brain that can complete text can power the
orchestra. The easiest route is **Ollama** (or vLLM / llama.cpp — anything
that serves an OpenAI-compatible HTTP endpoint):

```bash
# 1. install ollama (ollama.com), then pull a model:
ollama pull qwen2.5:7b        # or llama3.1:8b, mistral, phi4...
# it now serves http://localhost:11434/v1/chat/completions
```

Then write a third class in `llm.py` — same shape as the other two:

```python
class LocalLLM:
    """Open-source brain via any OpenAI-compatible server (Ollama, vLLM...)."""
    def __init__(self, base_url="http://localhost:11434/v1", model="qwen2.5:7b"):
        self.base_url, self.model = base_url, model

    async def complete(self, *, system, prompt, tier, meta) -> str:
        import aiohttp                      # or httpx; needs an async client
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{self.base_url}/chat/completions", json={
                "model": self.model,
                "messages": [{"role": "system", "content": system},
                             {"role": "user",   "content": prompt}],
            }) as r:
                data = await r.json()
                return data["choices"][0]["message"]["content"]
```

…and return it from `build_llm()`. That's the entire integration.

**What to expect when you do** (the honest caveats):

- **Concurrency is now your GPU.** Ollama serves requests mostly serially;
  set `--concurrency 1..2` or responses queue up server-side anyway. vLLM
  does continuous batching and genuinely eats parallel requests — it's the
  right server once you care about throughput.
- **JSON discipline is weaker** in small open models — the planner's "return
  ONLY a JSON array" will be ignored sooner. Our defensive `_parse_plan`
  fallback (orchestrator.py) stops that from killing the mission; with local
  models it goes from nice-to-have to essential.
- **Context windows are smaller** (often 8–32k). The `ctx_snippet_chars`
  truncation knob earns its keep.
- A 7B model is a *much* dumber agent — but the orchestration behaves
  identically, which is exactly the lesson: **intelligence and orchestration
  are separate layers.** You've already proven that with MockLLM, the
  dumbest model of all.

## 9. Where to go next

You've now hand-rolled the concepts these systems productionise:

- **Anthropic's "Building Effective Agents"** essay — the
  orchestrator-workers / routing / evaluator-optimizer patterns formalised.
- **Claude Agent SDK / Managed Agents** — agents-as-a-service: the loop,
  container, and tool execution run hosted, you write the config.
- **Celery / RQ** — this repo's queues+workers+retries+DLQ as battle-tested
  Python infrastructure (swap asyncio tasks for distributed processes).
- **Temporal** — durable workflows: our phase machine, but it survives
  process crashes.
- **Erlang/OTP** — the 1986 original: supervision trees, "let it crash",
  message-passing actors. Everything old is new again.
- **LangGraph / CrewAI / AutoGen** — agent-framework flavours of the same
  WORKFLOW-table idea; you'll now see straight through their abstractions.

---

*Layout:* `main.py` (entry) · `orchestra/messages.py` (protocol+queues) ·
`blackboard.py` (shared memory) · `llm.py` (mock & real brains) ·
`config.py` (the 25 roles + workflow + knobs) · `agent.py` (THE loop) ·
`orchestrator.py` (dispatch, supervisor, phases, stats) · `log.py`
(observability). Outputs land in `runs/<timestamp>/`.
