from __future__ import annotations

from pathlib import Path

import pytest

from trasep.config import Config, Paths, load_config
from trasep.io import read_delta
from trasep.pipelines.batch import run_all
from trasep.sample_data import generate_batch


@pytest.mark.integration
def test_batch_medallion_writes_delta_tables(spark, tmp_path: Path):
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
    generate_batch(config, files=2, rows_per_file=12, seed=21)
    run_all(spark, config)

    bronze = read_delta(spark, config.paths.bronze("orders"))
    silver = read_delta(spark, config.paths.silver("orders"))
    gold = read_delta(spark, config.paths.gold("daily_sales"))
    ltv = read_delta(spark, config.paths.gold("customer_ltv"))

    assert bronze.count() == 24
    assert silver.count() > 0
    assert silver.filter("is_valid = false").count() == 0
    assert gold.count() > 0
    assert ltv.count() > 0
    assert (config.paths.bronze("orders") / "_delta_log").exists()
