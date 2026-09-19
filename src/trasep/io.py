from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from trasep.config import to_spark_path


def read_json(spark: SparkSession, path: Path | str, schema: StructType) -> DataFrame:
    return spark.read.schema(schema).option("multiLine", "false").json(to_spark_path(path))


def read_json_stream(
    spark: SparkSession,
    path: Path | str,
    schema: StructType,
    max_files_per_trigger: int,
) -> DataFrame:
    return (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", str(max_files_per_trigger))
        .option("multiLine", "false")
        .json(to_spark_path(path))
    )


def read_delta(spark: SparkSession, path: Path | str) -> DataFrame:
    return spark.read.format("delta").load(to_spark_path(path))


def read_delta_stream(spark: SparkSession, path: Path | str) -> DataFrame:
    return spark.readStream.format("delta").option("ignoreChanges", "true").load(to_spark_path(path))


def empty_df(spark: SparkSession, schema: StructType, scratch: Path | str) -> DataFrame:
    scratch_path = Path(scratch)
    scratch_path.mkdir(parents=True, exist_ok=True)
    return spark.read.schema(schema).option("multiLine", "false").json(to_spark_path(scratch_path))


def ensure_delta_table(
    spark: SparkSession,
    path: Path | str,
    schema: StructType,
    partition_by: list[str] | None = None,
) -> None:
    target = Path(path)
    if (target / "_delta_log").exists():
        return
    df = empty_df(spark, schema, target.parent / f".empty_{target.name}")
    write_delta(df, path, mode="overwrite", partition_by=partition_by)


def write_delta(
    df: DataFrame,
    path: Path | str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    merge_schema: bool = True,
) -> None:
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true" if mode == "overwrite" else "false")
        .option("mergeSchema", "true" if merge_schema else "false")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(to_spark_path(path))


def write_delta_stream(
    df: DataFrame,
    path: Path | str,
    checkpoint: Path | str,
    query_name: str,
    trigger_seconds: int,
    partition_by: list[str] | None = None,
    output_mode: str = "append",
):
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", to_spark_path(checkpoint))
        .option("mergeSchema", "true")
        .queryName(query_name)
        .trigger(processingTime=f"{trigger_seconds} seconds")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.start(to_spark_path(path))
