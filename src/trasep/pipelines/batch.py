from __future__ import annotations

from pyspark.sql import SparkSession

from trasep.config import Config
from trasep.io import read_delta, read_json, write_delta
from trasep.schemas import ORDERS_RAW
from trasep.transforms import bronze_orders, gold_customer_ltv, gold_daily_sales, silver_orders


def run_bronze(spark: SparkSession, config: Config) -> None:
    landing = config.paths.landing_batch
    landing.mkdir(parents=True, exist_ok=True)
    raw = read_json(spark, landing, ORDERS_RAW)
    bronze = bronze_orders(raw)
    write_delta(bronze, config.paths.bronze("orders"), mode="overwrite", partition_by=["event_date"])


def run_silver(spark: SparkSession, config: Config) -> None:
    bronze = read_delta(spark, config.paths.bronze("orders"))
    silver = silver_orders(bronze, config.quality)
    write_delta(silver, config.paths.silver("orders"), mode="overwrite", partition_by=["event_date"])


def run_gold(spark: SparkSession, config: Config) -> None:
    silver = read_delta(spark, config.paths.silver("orders"))
    write_delta(gold_daily_sales(silver), config.paths.gold("daily_sales"), mode="overwrite")
    write_delta(gold_customer_ltv(silver), config.paths.gold("customer_ltv"), mode="overwrite")


def run_all(spark: SparkSession, config: Config) -> None:
    run_bronze(spark, config)
    run_silver(spark, config)
    run_gold(spark, config)
