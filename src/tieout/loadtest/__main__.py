"""CLI entry point for the load test — two subcommands:

    # Sweep rates at a fixed worker count, stop at the first bottleneck.
    uv run tieout-loadtest rate-sweep --rates 10 50 100 --duration 10

    # Fixed rate, sweep worker count — the horizontal-scaling demonstration.
    uv run tieout-loadtest worker-sweep --rate 100 --worker-counts 1 2 4 8 --duration 10

Both also run via `uv run python -m tieout.loadtest ...`. Requires Redis
running locally (docker run -p 6379:6379 redis:7-alpine).
"""

import argparse
import asyncio

from tieout.loadtest.harness import (
    DEFAULT_DRAIN_TIMEOUT_S,
    DEFAULT_NUM_WORKERS,
    run_load_test,
    sweep_worker_counts,
)
from tieout.logging_config import configure_logging


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep ingestion rate or worker count against a real Redis Streams queue "
        "and report the result with evidence (XLEN over time, per-event latency)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    rate_p = sub.add_parser("rate-sweep", help="Sweep rates at a fixed worker count; stop at the first bottleneck.")
    rate_p.add_argument(
        "--rates", type=float, nargs="+", required=True,
        help="Rates to test, in events/sec (e.g. --rates 10 50 100). Tested in ascending order.",
    )
    rate_p.add_argument("--duration", type=float, default=10.0, help="Seconds to push events at each rate.")
    rate_p.add_argument("--workers", type=int, default=DEFAULT_NUM_WORKERS, help="Worker coroutines to run.")
    rate_p.add_argument(
        "--drain-timeout", type=float, default=DEFAULT_DRAIN_TIMEOUT_S,
        help="Seconds to wait for the backlog to empty before calling a rate a bottleneck.",
    )
    rate_p.add_argument("--seed", type=int, default=None, help="Simulator seed, to reproduce a specific run.")

    worker_p = sub.add_parser("worker-sweep", help="Fixed rate, sweep worker count — horizontal-scaling story.")
    worker_p.add_argument("--rate", type=float, required=True, help="Fixed events/sec rate to hold across the sweep.")
    worker_p.add_argument(
        "--worker-counts", type=int, nargs="+", required=True,
        help="Worker counts to test (e.g. --worker-counts 1 2 4 8). Runs every one, no early stop.",
    )
    worker_p.add_argument("--duration", type=float, default=10.0, help="Seconds to push events at each worker count.")
    worker_p.add_argument(
        "--drain-timeout", type=float, default=DEFAULT_DRAIN_TIMEOUT_S,
        help="Seconds to wait for the backlog to empty before calling that worker count saturated.",
    )
    worker_p.add_argument("--seed", type=int, default=None, help="Simulator seed, to reproduce a specific run.")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    args = _parse_args(argv)

    if args.command == "rate-sweep":
        report = asyncio.run(
            run_load_test(
                rates=sorted(args.rates),
                duration_s=args.duration,
                num_workers=args.workers,
                drain_timeout_s=args.drain_timeout,
                seed=args.seed,
            )
        )
    else:
        report = asyncio.run(
            sweep_worker_counts(
                rate=args.rate,
                worker_counts=sorted(args.worker_counts),
                duration_s=args.duration,
                drain_timeout_s=args.drain_timeout,
                seed=args.seed,
            )
        )
    print(report.summary())


if __name__ == "__main__":
    main()
