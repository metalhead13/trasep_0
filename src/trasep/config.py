from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "pipeline.yaml"


def to_spark_path(path: Path | str) -> str:
    """Return a Spark-friendly absolute path with forward slashes."""
    return Path(path).resolve().as_posix()


@dataclass(frozen=True)
class Paths:
    root: Path
    landing: Path
    lakehouse: Path
    checkpoints: Path
    tools: Path

    @property
    def landing_batch(self) -> Path:
        return self.landing / "batch"

    @property
    def landing_stream(self) -> Path:
        return self.landing / "stream"

    def bronze(self, table: str) -> Path:
        return self.lakehouse / "bronze" / table

    def silver(self, table: str) -> Path:
        return self.lakehouse / "silver" / table

    def gold(self, table: str) -> Path:
        return self.lakehouse / "gold" / table

    def checkpoint(self, name: str) -> Path:
        return self.checkpoints / name


@dataclass(frozen=True)
class SparkSettings:
    app_name: str
    master: str
    shuffle_partitions: int
    timezone: str


@dataclass(frozen=True)
class StreamingSettings:
    trigger_seconds: int
    max_files_per_trigger: int
    watermark_minutes: int
    emit_interval_seconds: int
    emit_files: int


@dataclass(frozen=True)
class QualitySettings:
    allowed_status: tuple[str, ...]
    allowed_channels: tuple[str, ...]
    allowed_currencies: tuple[str, ...]


@dataclass(frozen=True)
class Config:
    paths: Paths
    spark: SparkSettings
    streaming: StreamingSettings
    quality: QualitySettings
    raw: dict[str, Any]


def load_config(config_path: Path | None = None, root: Path | None = None) -> Config:
    root = (root or ROOT).resolve()
    config_path = config_path or DEFAULT_CONFIG
    with config_path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    path_cfg = raw.get("paths", {})
    spark_cfg = raw.get("spark", {})
    stream_cfg = raw.get("streaming", {})
    quality_cfg = raw.get("quality", {})

    paths = Paths(
        root=root,
        landing=(root / path_cfg.get("landing", "data/landing")).resolve(),
        lakehouse=(root / path_cfg.get("lakehouse", "data/lakehouse")).resolve(),
        checkpoints=(root / path_cfg.get("checkpoints", "data/checkpoints")).resolve(),
        tools=(root / path_cfg.get("tools", ".tools")).resolve(),
    )
    spark = SparkSettings(
        app_name=spark_cfg.get("app_name", "trasep-lakehouse"),
        master=spark_cfg.get("master", "local[*]"),
        shuffle_partitions=int(spark_cfg.get("shuffle_partitions", 4)),
        timezone=spark_cfg.get("timezone", "UTC"),
    )
    streaming = StreamingSettings(
        trigger_seconds=int(stream_cfg.get("trigger_seconds", 5)),
        max_files_per_trigger=int(stream_cfg.get("max_files_per_trigger", 8)),
        watermark_minutes=int(stream_cfg.get("watermark_minutes", 10)),
        emit_interval_seconds=int(stream_cfg.get("emit_interval_seconds", 3)),
        emit_files=int(stream_cfg.get("emit_files", 6)),
    )
    quality = QualitySettings(
        allowed_status=tuple(quality_cfg.get("allowed_status", ())),
        allowed_channels=tuple(quality_cfg.get("allowed_channels", ())),
        allowed_currencies=tuple(quality_cfg.get("allowed_currencies", ())),
    )
    return Config(paths=paths, spark=spark, streaming=streaming, quality=quality, raw=raw)
