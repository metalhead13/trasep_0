from __future__ import annotations

import time
from pathlib import Path

import pytest

from trasep.config import Config, Paths, load_config
from trasep.io import read_delta
from trasep.pipelines.streaming import start_bronze
from trasep.sample_data import generate_stream_file


@pytest.mark.integration
def test_streaming_bronze_writes_delta(spark, tmp_path: Path):
    base = load_config()
    config = Config(
        paths=Paths(
            root=tmp_path,
            landing=tmp_path / "landing",
            lakehouse=tmp_path / "lakehouse",
            checkpoints=tmp_path / "checkpoints",
            tools=base.paths.tools,
        ),
        spark=base.spark,
        streaming=base.streaming,
        quality=base.quality,
        raw=base.raw,
    )
    query = start_bronze(spark, config)
    try:
        generate_stream_file(config, file_idx=0, rows=6, seed=3)
        bronze_path = config.paths.bronze("orders_stream")
        rows = 0
        for _ in range(30):
            if (bronze_path / "_delta_log").exists():
                rows = read_delta(spark, bronze_path).count()
                if rows > 0:
                    break
            time.sleep(1)
        assert rows > 0
    finally:
        if query.isActive:
            query.stop()
        query.awaitTermination(10)
