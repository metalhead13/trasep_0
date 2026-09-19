from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from trasep.config import QualitySettings


def _in_set(column: str, values: tuple[str, ...]) -> Column:
    return F.lower(F.col(column)).isin([value.lower() for value in values])


def with_quality_flags(df: DataFrame, quality: QualitySettings) -> DataFrame:
    missing_keys = (
        F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == "")
        | F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("event_time").isNull()
    )
    invalid_measures = (
        F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
    )
    invalid_enums = (
        ~_in_set("status", quality.allowed_status)
        | ~_in_set("channel", quality.allowed_channels)
        | ~_in_set("currency", quality.allowed_currencies)
    )
    return (
        df.withColumn("dq_missing_keys", missing_keys)
        .withColumn("dq_invalid_measures", invalid_measures)
        .withColumn("dq_invalid_enums", invalid_enums)
        .withColumn(
            "is_valid",
            ~(F.col("dq_missing_keys") | F.col("dq_invalid_measures") | F.col("dq_invalid_enums")),
        )
        .withColumn(
            "dq_reason",
            F.when(missing_keys, F.lit("missing_keys"))
            .when(invalid_measures, F.lit("invalid_measures"))
            .when(invalid_enums, F.lit("invalid_enums"))
            .otherwise(F.lit(None).cast("string")),
        )
    )
