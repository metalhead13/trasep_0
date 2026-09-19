from __future__ import annotations

from trasep.config import load_config


def test_load_config_reads_quality_rules():
    config = load_config()
    assert "paid" in config.quality.allowed_status
    assert config.paths.landing_batch.name == "batch"
    assert config.spark.app_name == "trasep-lakehouse"
