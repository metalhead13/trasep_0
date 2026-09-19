from __future__ import annotations

from pathlib import Path

from trasep.config import QualitySettings
from trasep.io import read_json
from trasep.sample_data import write_jsonl
from trasep.schemas import BRONZE_ORDERS, SILVER_ORDERS
from trasep.transforms import gold_daily_sales, silver_orders


QUALITY = QualitySettings(
    allowed_status=("placed", "paid", "shipped", "delivered", "cancelled", "returned"),
    allowed_channels=("web", "mobile", "store", "marketplace"),
    allowed_currencies=("USD", "EUR", "MXN"),
)


def test_silver_keeps_latest_valid_event(spark, tmp_path: Path):
    write_jsonl(
        tmp_path / "bronze.json",
        [
            {
                "order_id": "ord-1",
                "customer_id": "c001",
                "product_id": "p100",
                "product_name": "Trail Pack",
                "category": "outdoor",
                "channel": "WEB",
                "status": "placed",
                "quantity": 2,
                "unit_price": 10.0,
                "currency": "usd",
                "country": "us",
                "event_time": "2026-09-18T12:00:00Z",
                "ingest_batch": "2026091812",
                "ingest_ts": "2026-09-18T12:00:00Z",
                "event_date": "2026-09-18",
                "source_file": "file://orders.json",
            },
            {
                "order_id": "ord-1",
                "customer_id": "c001",
                "product_id": "p100",
                "product_name": "Trail Pack",
                "category": "outdoor",
                "channel": "web",
                "status": "paid",
                "quantity": 2,
                "unit_price": 10.0,
                "currency": "USD",
                "country": "US",
                "event_time": "2026-09-18T13:00:00Z",
                "ingest_batch": "2026091813",
                "ingest_ts": "2026-09-18T13:00:00Z",
                "event_date": "2026-09-18",
                "source_file": "file://orders.json",
            },
            {
                "order_id": "",
                "customer_id": "c002",
                "product_id": "p200",
                "product_name": "City Tote",
                "category": "lifestyle",
                "channel": "web",
                "status": "paid",
                "quantity": 1,
                "unit_price": 20.0,
                "currency": "USD",
                "country": "MX",
                "event_time": "2026-09-18T13:00:00Z",
                "ingest_batch": "2026091813",
                "ingest_ts": "2026-09-18T13:00:00Z",
                "event_date": "2026-09-18",
                "source_file": "file://orders.json",
            },
        ],
    )
    bronze = read_json(spark, tmp_path / "bronze.json", BRONZE_ORDERS)
    silver = silver_orders(bronze, QUALITY)
    assert silver.count() == 2
    current = silver.filter("is_current = true").select("status", "gross_amount").collect()
    assert current[0].status == "paid"
    assert current[0].gross_amount == 20.0


def test_gold_excludes_cancelled_current_orders(spark, tmp_path: Path):
    write_jsonl(
        tmp_path / "silver.json",
        [
            {
                "order_id": "ORD-1",
                "customer_id": "C001",
                "product_id": "P100",
                "product_name": "Trail Pack",
                "category": "Outdoor",
                "channel": "web",
                "status": "paid",
                "quantity": 2,
                "unit_price": 10.0,
                "currency": "USD",
                "country": "US",
                "event_time": "2026-09-18T12:00:00Z",
                "ingest_batch": "2026091812",
                "ingest_ts": "2026-09-18T12:00:00Z",
                "event_date": "2026-09-18",
                "source_file": "file://orders.json",
                "gross_amount": 20.0,
                "dq_missing_keys": False,
                "dq_invalid_measures": False,
                "dq_invalid_enums": False,
                "is_valid": True,
                "dq_reason": None,
                "is_current": True,
            },
            {
                "order_id": "ORD-2",
                "customer_id": "C002",
                "product_id": "P200",
                "product_name": "City Tote",
                "category": "Outdoor",
                "channel": "web",
                "status": "cancelled",
                "quantity": 1,
                "unit_price": 50.0,
                "currency": "USD",
                "country": "US",
                "event_time": "2026-09-18T12:00:00Z",
                "ingest_batch": "2026091812",
                "ingest_ts": "2026-09-18T12:00:00Z",
                "event_date": "2026-09-18",
                "source_file": "file://orders.json",
                "gross_amount": 50.0,
                "dq_missing_keys": False,
                "dq_invalid_measures": False,
                "dq_invalid_enums": False,
                "is_valid": True,
                "dq_reason": None,
                "is_current": True,
            },
        ],
    )
    silver = read_json(spark, tmp_path / "silver.json", SILVER_ORDERS)
    gold = gold_daily_sales(silver)
    assert gold.count() == 1
    row = gold.select("orders", "revenue").collect()[0]
    assert row.orders == 1
    assert row.revenue == 20.0

