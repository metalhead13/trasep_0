from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from trasep.config import QualitySettings
from trasep.quality import with_quality_flags


def bronze_orders(raw: DataFrame) -> DataFrame:
    return (
        raw.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("source_file", F.input_file_name())
    )


def _normalize(bronze: DataFrame) -> DataFrame:
    return (
        bronze.withColumn("order_id", F.upper(F.trim("order_id")))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("product_id", F.upper(F.trim("product_id")))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("channel", F.lower(F.trim("channel")))
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )


def silver_orders(
    bronze: DataFrame,
    quality: QualitySettings,
    watermark_minutes: int | None = None,
) -> DataFrame:
    cleaned = _normalize(bronze)
    if watermark_minutes is not None:
        cleaned = cleaned.withWatermark("event_time", f"{watermark_minutes} minutes")
    cleaned = cleaned.dropDuplicates(["order_id", "event_time", "status"])
    flagged = with_quality_flags(cleaned, quality).filter(F.col("is_valid"))
    if watermark_minutes is not None:
        return flagged.withColumn("is_current", F.lit(True))
    window = Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
    return (
        flagged.withColumn("event_rank", F.row_number().over(window))
        .withColumn("is_current", F.col("event_rank") == 1)
        .drop("event_rank")
    )


def gold_daily_sales(silver: DataFrame) -> DataFrame:
    current = silver.filter(F.col("is_current") & ~F.col("status").isin("cancelled", "returned"))
    return (
        current.groupBy("event_date", "country", "category", "channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("gross_amount"), 2).alias("revenue"),
            F.round(F.avg("gross_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("load_ts", F.current_timestamp())
    )


def gold_customer_ltv(silver: DataFrame) -> DataFrame:
    current = silver.filter(F.col("is_current") & ~F.col("status").isin("cancelled", "returned"))
    return (
        current.groupBy("customer_id", "country")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("gross_amount"), 2).alias("lifetime_revenue"),
            F.min("event_time").alias("first_order_ts"),
            F.max("event_time").alias("last_order_ts"),
        )
        .withColumn("load_ts", F.current_timestamp())
    )


def gold_streaming_kpis(silver: DataFrame, watermark_minutes: int) -> DataFrame:
    return (
        silver.filter(F.col("is_valid"))
        .withWatermark("event_time", f"{watermark_minutes} minutes")
        .groupBy(
            F.window("event_time", "5 minutes").alias("kpi_window"),
            "country",
            "channel",
        )
        .agg(
            F.count(F.lit(1)).alias("orders"),
            F.round(F.sum("gross_amount"), 2).alias("revenue"),
        )
        .select(
            F.col("kpi_window.start").alias("window_start"),
            F.col("kpi_window.end").alias("window_end"),
            "country",
            "channel",
            "orders",
            "revenue",
            F.current_timestamp().alias("load_ts"),
        )
    )
