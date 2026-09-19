from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from trasep.config import Config, to_spark_path

WINUTILS_BASE = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
WINUTILS_FILES = ("winutils.exe", "hadoop.dll")


def _download(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    urllib.request.urlretrieve(url, tmp)
    tmp.replace(destination)


def _ensure_hadoop_home(tools_dir: Path) -> None:
    """Install Hadoop Windows binaries so Spark can list local files."""
    if os.name != "nt":
        return
    bin_dir = tools_dir / "hadoop" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for name in WINUTILS_FILES:
        target = bin_dir / name
        if target.exists() and target.stat().st_size > 0:
            continue
        _download(f"{WINUTILS_BASE}/{name}", target)
    hadoop_home = str(tools_dir / "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")


def _prefer_jdk17() -> None:
    jdk17 = Path(r"C:\Program Files\Java\jdk-17")
    if jdk17.exists():
        os.environ["JAVA_HOME"] = str(jdk17)
        os.environ["PATH"] = str(jdk17 / "bin") + os.pathsep + os.environ.get("PATH", "")


def _pin_python_worker() -> str:
    python = sys.executable
    os.environ["PYSPARK_PYTHON"] = python
    os.environ["PYSPARK_DRIVER_PYTHON"] = python
    return python


def build_spark(config: Config, app_name: str | None = None) -> SparkSession:
    _ensure_hadoop_home(config.paths.tools)
    _prefer_jdk17()
    python = _pin_python_worker()
    os.environ.setdefault("TZ", config.spark.timezone)

    warehouse = to_spark_path(config.paths.lakehouse / "warehouse")
    builder = (
        SparkSession.builder.appName(app_name or config.spark.app_name)
        .master(config.spark.master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", config.spark.timezone)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python)
        .config("spark.pyspark.driver.python", python)
        .config("spark.executorEnv.PYSPARK_PYTHON", python)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.reuse", "true")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
