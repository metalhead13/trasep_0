# Trasep lakehouse

PySpark + Delta Lake pipelines that follow a medallion architecture:

- **Bronze**: raw JSON orders landed as-is, plus ingest metadata.
- **Silver**: typed, deduplicated, quality-checked current order events.
- **Gold**: analytics tables for daily sales, customer LTV, and streaming KPIs.

Batch and streaming jobs share the same transforms so both paths produce comparable silver/gold contracts.

## Architecture

```text
landing JSON ──► bronze.orders ──► silver.orders ──► gold.daily_sales
     │                                    │          gold.customer_ltv
     └─ stream files ──► bronze.orders_stream
                         silver.orders_stream
                         gold.streaming_kpis
```

| Layer | Table | Grain | Notes |
| --- | --- | --- | --- |
| Bronze | `orders` / `orders_stream` | one raw event | Keeps source file, ingest timestamp, event date |
| Silver | `orders` / `orders_stream` | one valid event | Drops invalid rows. Batch flags the latest event per `order_id`; streaming keeps watermarked unique events |
| Gold | `daily_sales` | date + country + category + channel | Excludes cancelled/returned current orders |
| Gold | `customer_ltv` | customer + country | Lifetime paid demand |
| Gold | `streaming_kpis` | 5-minute window + country + channel | Watermarked streaming aggregates |

## Requirements

- Python 3.10+
- JDK 17 (`JAVA_HOME` should point at it; `src/trasep/spark.py` prefers `C:\Program Files\Java\jdk-17` when present)
- Windows users: Spark needs Hadoop native binaries. The session builder downloads `winutils.exe` and `hadoop.dll` into `.tools/hadoop/bin` on first run and pins `PYSPARK_PYTHON` to the active interpreter.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Run batch

```powershell
python -m trasep seed
python -m trasep batch
```

If landing files are missing, `batch` seeds sample JSON automatically.

Outputs:

- `data/lakehouse/bronze/orders`
- `data/lakehouse/silver/orders`
- `data/lakehouse/gold/daily_sales`
- `data/lakehouse/gold/customer_ltv`

## Run streaming

```powershell
python -m trasep stream --emit --seconds 45
```

`--emit` writes sample JSON into `data/landing/stream` while bronze, silver, and gold streaming queries run. Checkpoints live under `data/checkpoints`.

Use `--seconds 0` to wait until you stop the process.

## Tests

```powershell
python -m pytest
```

Spark tests read JSON fixtures instead of `createDataFrame` so local Windows runs stay on the JVM execution path. `tests/test_batch_pipeline.py` writes temporary Delta tables.

## Project layout

```text
config/pipeline.yaml          # paths, Spark, streaming, quality rules
src/trasep/config.py          # typed configuration
src/trasep/spark.py           # Delta-enabled SparkSession
src/trasep/transforms.py      # shared bronze/silver/gold logic
src/trasep/pipelines/batch.py
src/trasep/pipelines/streaming.py
src/trasep/sample_data.py     # local JSON generator
tests/
```
# trasep_0
