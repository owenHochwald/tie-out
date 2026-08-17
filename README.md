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
