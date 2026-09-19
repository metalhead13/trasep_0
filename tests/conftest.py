from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from trasep.config import load_config
from trasep.spark import build_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    config = load_config()
    session = build_spark(config, app_name="trasep-tests")
    yield session
    session.stop()
