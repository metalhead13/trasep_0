from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_RAW = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingest_batch", StringType(), True),
    ]
)

BRONZE_ORDERS = StructType(
    ORDERS_RAW.fields
    + [
        StructField("ingest_ts", TimestampType(), True),
        StructField("event_date", DateType(), True),
        StructField("source_file", StringType(), True),
    ]
)

SILVER_ORDERS = StructType(
    BRONZE_ORDERS.fields
    + [
        StructField("gross_amount", DoubleType(), True),
        StructField("dq_missing_keys", BooleanType(), True),
        StructField("dq_invalid_measures", BooleanType(), True),
        StructField("dq_invalid_enums", BooleanType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("dq_reason", StringType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

GOLD_STREAMING_KPIS = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("orders", LongType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("load_ts", TimestampType(), True),
    ]
)
