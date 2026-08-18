"""CLI entry point for the load test:

    uv run python -m tieout.loadtest --rates 10 50 100 --duration 10
    uv run tieout-loadtest --rates 10 50 100 --duration 10   # same, via [project.scripts]

Requires Redis running locally (docker run -p 6379:6379 redis:7-alpine).
"""

import argparse
import asyncio

from tieout.loadtest.harness import (
    DEFAULT_DRAIN_TIMEOUT_S,
    DEFAULT_NUM_WORKERS,
    run_load_test,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep ingestion rates against a real Redis Streams queue and report "
        "the sustained-throughput bottleneck, with evidence (XLEN over time, per-event latency)."
    )
    parser.add_argument(
        "--rates", type=float, nargs="+", required=True,
        help="Rates to test, in events/sec (e.g. --rates 10 50 100). Tested in ascending order.",
    )
    parser.add_argument("--duration", type=float, default=10.0, help="Seconds to push events at each rate.")
    parser.add_argument("--workers", type=int, default=DEFAULT_NUM_WORKERS, help="Worker coroutines to run.")
    parser.add_argument(
        "--drain-timeout", type=float, default=DEFAULT_DRAIN_TIMEOUT_S,
        help="Seconds to wait for the backlog to empty before calling a rate a bottleneck.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Simulator seed, to reproduce a specific run.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    report = asyncio.run(
        run_load_test(
            rates=sorted(args.rates),
            duration_s=args.duration,
            num_workers=args.workers,
            drain_timeout_s=args.drain_timeout,
            seed=args.seed,
        )
    )
    print(report.summary())


if __name__ == "__main__":
    main()
