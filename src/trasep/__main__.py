from __future__ import annotations

import argparse
import time
from pathlib import Path

from trasep.config import load_config
from trasep.pipelines.batch import run_all as run_batch
from trasep.pipelines.streaming import await_queries, start_all
from trasep.sample_data import generate_batch, generate_stream_file
from trasep.spark import build_spark


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trasep medallion lakehouse pipelines")
    parser.add_argument("--config", type=Path, default=None, help="Optional pipeline YAML")
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed", help="Write sample JSON landing files")
    seed.add_argument("--files", type=int, default=3)
    seed.add_argument("--rows", type=int, default=40)

    sub.add_parser("batch", help="Run bronze -> silver -> gold batch jobs")

    stream = sub.add_parser("stream", help="Start streaming bronze/silver/gold queries")
    stream.add_argument("--seconds", type=int, default=45, help="Run duration; 0 waits forever")
    stream.add_argument("--emit", action="store_true", help="Also emit sample stream files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)

    if args.command == "seed":
        written = generate_batch(config, files=args.files, rows_per_file=args.rows)
        print(f"Wrote {len(written)} landing files under {config.paths.landing_batch}")
        return 0

    spark = build_spark(config, app_name=f"trasep-{args.command}")
    try:
        if args.command == "batch":
            if not any(config.paths.landing_batch.glob("*.json")):
                generate_batch(config)
            run_batch(spark, config)
            print("Batch medallion run complete:")
            print(f"  bronze: {config.paths.bronze('orders')}")
            print(f"  silver: {config.paths.silver('orders')}")
            print(f"  gold:   {config.paths.gold('daily_sales')}")
            print(f"  gold:   {config.paths.gold('customer_ltv')}")
            return 0

        queries = start_all(spark, config)
        if args.emit:
            for idx in range(config.streaming.emit_files):
                generate_stream_file(config, idx)
                time.sleep(config.streaming.emit_interval_seconds)
        timeout = None if args.seconds == 0 else args.seconds
        await_queries(queries, timeout_seconds=timeout)
        print("Streaming queries stopped.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
