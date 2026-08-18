# tie-out

A reconciliation engine that ties out a **position book** (what we believe we own,
built from our own trade confirmations) against a **market feed** (what actually
happened, reported independently) — trade by trade, at volume, under duplicate
delivery and out-of-order arrival.

The full design rationale — why Redis Streams, the domain models, the
reconciliation taxonomy, the correctness hazards this is built to survive, and the
build order — lives in **[`CLAUDE.md`](./CLAUDE.md)**. Read that first; this file is
just how to get running.

## Prerequisites

- Python 3.12
- [`uv`](https://docs.astral.sh/uv/) for dependency management
- Docker (for Redis)

## Setup

```bash
# install dependencies (creates .venv automatically)
uv sync

# start Redis
docker run -p 6379:6379 redis:7-alpine

# copy the env template and fill in anything that differs from the defaults
cp .env.example .env
```

## Running things

```bash
# run the test suite
uv run pytest

# run any module/script
uv run python -m tieout.<module>
```

## Load testing

Two sweeps, both against a real local Redis — see `src/tieout/loadtest/`.
Each rate/worker-count trial starts from a clean stream, pushes real events
through real `worker_loop` coroutines, and reports two things per CLAUDE.md:
`XLEN` sampled over time (turned into an actual backlog signal — see below)
and per-event latency from `XADD` to `XACK`.

```bash
# Rate sweep: ascending rates, stop at the first that doesn't drain.
uv run tieout-loadtest rate-sweep --rates 10 50 100 --duration 10

# Worker sweep: fixed rate, vary worker count — the horizontal-scaling story.
uv run tieout-loadtest worker-sweep --rate 100 --worker-counts 1 2 4 8 --duration 10
```

```
   10000 evt/s  OK, drained                 peak backlog=2   final backlog=0   latency p50=0.000s p95=0.000s p99=0.000s max=0.007s
```

**Worth knowing before quoting a number from this**: Redis Streams never
shrink `XLEN` on `XACK` (only `XTRIM`/`XDEL` do), so raw `XLEN` alone can
never signal "caught up." The harness tracks a derived backlog — `XLEN`
minus acks observed so far — and that's what "drained" actually means here;
raw `XLEN` is still recorded, but only as informational stream-size data.
Also: the producer pushes one `XADD` per event, unpipelined, over a single
connection — at high requested rates *that loop*, not the reconciliation
pipeline, becomes the limiting factor, so a "drained" result at very high
rates says more about the load generator than about the queue/worker
system. A pipelined producer would be the natural next step to separate
those two ceilings cleanly.

## Project layout

```
src/tieout/
├── domain/          # TradeEvent, Side, EventSource, Break, BreakType — frozen, slotted, Decimal-based
├── ingest/           # synthetic event simulator (clean / chaos / volume configs)
├── queue/             # Redis Streams wiring: consumer group, worker loop, XAUTOCLAIM sweep
├── reconciliation/   # ReconciliationStore + taxonomy rules (docs/RECONCILIATION_RULES.md)
├── rollup/            # RollupAggregator — per-symbol position/notional, order-independent
├── loadtest/          # throughput + backlog (XLEN) + per-event latency measurement
└── reporting/         # reads break_log + rollups + structured logs, no separate DB
```

Build order and the reasoning behind each component are in `CLAUDE.md`.

## Project debrief

Trade reconciliation engine: ties a position book out against an
independently-reported market feed, trade by trade, under the conditions
that actually break naive implementations — duplicate delivery,
out-of-order arrival, worker crashes mid-processing.

- **At-least-once delivery, correctly.** Redis Streams consumer groups
  (`XREADGROUP` / `XACK` / `XAUTOCLAIM`) — a worker that dies between
  reading a message and acking it leaves it in the Pending Entries List;
  a sweep reclaims and redelivers it. Every apply-side function is
  idempotent by construction, not by convention — this is exercised
  directly, including the exact interleaving of "already applied, then
  redelivered."
- **Financial-safe by construction.** `Decimal` throughout for quantity
  and price — never `float` — so reconciliation breaks are real
  disagreements, not rounding artifacts. Immutable, slotted domain models.
- **Order-independence proven, not assumed.** Two independently-shuffled
  250,000-trade runs converge to bit-identical rollup state — a direct
  test in the suite, not a claim.
- **Honest load testing.** Rate and worker-count sweeps against a real
  Redis instance, reporting the actual bottleneck with evidence (a
  reconstructed backlog signal + latency percentiles) rather than a single
  headline throughput number — including correctly diagnosing when the
  load generator itself, not the system under test, is the limiting
  factor.
- **177 automated tests**, unit and integration (real Redis), including
  hazard-specific tests: a message redelivered after being fully applied
  but before its original ack is proven to no-op, and the sweep-vs-match
  interleaving (a trade resolved by its counterpart out of arrival order,
  mid-sweep) is proven sound. The concurrent-arrival race CLAUDE.md
  describes is prevented by construction — `apply()`/`sweep_stale()` never
  `await` mid-function — rather than guarded against at runtime, which is
  the documented, load-bearing reason this stays a single-process design.

*(One known gap, left as-is rather than glossed over: the in-process dedup
set has no retention window — CLAUDE.md calls this out as a hazard to
state explicitly, and it isn't bounded yet.)*
