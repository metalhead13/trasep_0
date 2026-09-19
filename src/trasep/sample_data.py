from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trasep.config import Config

CUSTOMERS = [f"C{idx:03d}" for idx in range(1, 21)]
PRODUCTS = [
    ("P100", "Trail Pack", "Outdoor"),
    ("P200", "City Tote", "Lifestyle"),
    ("P300", "Studio Lamp", "Home"),
    ("P400", "Aero Bottle", "Sports"),
    ("P500", "Notebook Pro", "Office"),
]
CHANNELS = ["web", "mobile", "store", "marketplace"]
STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled", "returned"]
CURRENCIES = ["USD", "EUR", "MXN"]
COUNTRIES = ["US", "MX", "ES", "DE"]


def _event(index: int, now: datetime, dirty: bool = False) -> dict:
    product_id, product_name, category = random.choice(PRODUCTS)
    event_time = now - timedelta(minutes=random.randint(0, 180))
    payload = {
        "order_id": f"ORD-{index:05d}",
        "customer_id": random.choice(CUSTOMERS),
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "channel": random.choice(CHANNELS),
        "status": random.choice(STATUSES),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(12.5, 180.0), 2),
        "currency": random.choice(CURRENCIES),
        "country": random.choice(COUNTRIES),
        "event_time": event_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "ingest_batch": event_time.strftime("%Y%m%d%H"),
    }
    if dirty:
        kind = index % 3
        if kind == 0:
            payload["order_id"] = ""
        elif kind == 1:
            payload["quantity"] = 0
        else:
            payload["status"] = "unknown"
    return payload


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def generate_batch(config: Config, files: int = 3, rows_per_file: int = 40, seed: int = 7) -> list[Path]:
    random.seed(seed)
    now = datetime.now(timezone.utc)
    written: list[Path] = []
    for file_idx in range(files):
        records = []
        for row_idx in range(rows_per_file):
            index = file_idx * rows_per_file + row_idx + 1
            records.append(_event(index, now, dirty=row_idx % 11 == 0))
        path = config.paths.landing_batch / f"orders_{file_idx:02d}.json"
        write_jsonl(path, records)
        written.append(path)
    return written


def generate_stream_file(config: Config, file_idx: int, rows: int = 8, seed: int | None = None) -> Path:
    if seed is not None:
        random.seed(seed + file_idx)
    now = datetime.now(timezone.utc)
    records = [_event(10_000 + file_idx * 100 + row, now, dirty=row == 0) for row in range(rows)]
    path = config.paths.landing_stream / f"orders_live_{file_idx:04d}.json"
    write_jsonl(path, records)
    return path
