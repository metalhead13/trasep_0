from __future__ import annotations

import time

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from trasep.config import Config
from trasep.io import ensure_delta_table, read_delta_stream, read_json_stream, write_delta_stream
from trasep.schemas import BRONZE_ORDERS, GOLD_STREAMING_KPIS, ORDERS_RAW, SILVER_ORDERS
from trasep.transforms import bronze_orders, gold_streaming_kpis, silver_orders


def start_bronze(spark: SparkSession, config: Config) -> StreamingQuery:
    config.paths.landing_stream.mkdir(parents=True, exist_ok=True)
    ensure_delta_table(spark, config.paths.bronze("orders_stream"), BRONZE_ORDERS, ["event_date"])
    raw = read_json_stream(
        spark,
        config.paths.landing_stream,
        ORDERS_RAW,
        config.streaming.max_files_per_trigger,
    )
    bronze = bronze_orders(raw)
    return write_delta_stream(
        bronze,
        config.paths.bronze("orders_stream"),
        config.paths.checkpoint("bronze_orders"),
        query_name="bronze_orders",
        trigger_seconds=config.streaming.trigger_seconds,
        partition_by=["event_date"],
    )


def start_silver(spark: SparkSession, config: Config) -> StreamingQuery:
    ensure_delta_table(spark, config.paths.silver("orders_stream"), SILVER_ORDERS, ["event_date"])
    bronze = read_delta_stream(spark, config.paths.bronze("orders_stream"))
    silver = silver_orders(bronze, config.quality, watermark_minutes=config.streaming.watermark_minutes)
    return write_delta_stream(
        silver,
        config.paths.silver("orders_stream"),
        config.paths.checkpoint("silver_orders"),
        query_name="silver_orders",
        trigger_seconds=config.streaming.trigger_seconds,
        partition_by=["event_date"],
    )


def start_gold(spark: SparkSession, config: Config) -> StreamingQuery:
    ensure_delta_table(spark, config.paths.gold("streaming_kpis"), GOLD_STREAMING_KPIS)
    silver = read_delta_stream(spark, config.paths.silver("orders_stream"))
    gold = gold_streaming_kpis(silver, config.streaming.watermark_minutes)
    return write_delta_stream(
        gold,
        config.paths.gold("streaming_kpis"),
        config.paths.checkpoint("gold_kpis"),
        query_name="gold_streaming_kpis",
        trigger_seconds=config.streaming.trigger_seconds,
        output_mode="append",
    )


def start_all(spark: SparkSession, config: Config) -> list[StreamingQuery]:
    bronze = start_bronze(spark, config)
    silver = start_silver(spark, config)
    gold = start_gold(spark, config)
    return [bronze, silver, gold]


def await_queries(queries: list[StreamingQuery], timeout_seconds: int | None = None) -> None:
    if timeout_seconds is None:
        for query in queries:
            query.awaitTermination()
        return
    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            if any(not query.isActive for query in queries):
                break
            time.sleep(1)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        for query in queries:
            query.awaitTermination(10)
