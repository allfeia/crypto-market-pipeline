from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


def manifest_key(object_key: str) -> str:
    if not object_key.endswith(".parquet"):
        raise ValueError("Parquet object key must end with .parquet")
    return f"{object_key[:-len('.parquet')]}.manifest.json"


def _utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def event_id_digest(rows: list[dict[str, Any]]) -> str:
    identity = "\n".join(sorted(row["event_id"] for row in rows))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def event_id_xor(event_ids: set[str]) -> str:
    checksum = 0
    for event_id in event_ids:
        checksum ^= int(hashlib.sha256(event_id.encode("utf-8")).hexdigest(), 16)
    return f"{checksum:064x}"


def build_manifest(object_key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError("Cannot build a manifest for an empty batch")

    event_times = [_utc_timestamp(row["event_time"]) for row in rows]
    ingest_times = [_utc_timestamp(row["ingest_time"]) for row in rows]
    grouped: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(lambda: {"row_count": 0, "event_ids": set()})
    for row in rows:
        key = (row["exchange"], row["symbol"], row["event_type"])
        grouped[key]["row_count"] += 1
        grouped[key]["event_ids"].add(row["event_id"])

    groups = [
        {
            "exchange": exchange,
            "symbol": symbol,
            "event_type": event_type,
            "row_count": values["row_count"],
            "unique_event_ids": len(values["event_ids"]),
            "event_id_digest": hashlib.sha256(
                "\n".join(sorted(values["event_ids"])).encode("utf-8")
            ).hexdigest(),
            "event_id_xor": event_id_xor(values["event_ids"]),
        }
        for (exchange, symbol, event_type), values in sorted(grouped.items())
    ]
    batch_hash = object_key.rsplit("part-", 1)[-1].removesuffix(".parquet")
    return {
        "schema_version": 2,
        "object_key": object_key,
        "manifest_key": manifest_key(object_key),
        "batch_hash": batch_hash,
        "row_count": len(rows),
        "unique_event_ids": len({row["event_id"] for row in rows}),
        "event_id_digest": event_id_digest(rows),
        "min_event_time": _format_timestamp(min(event_times)),
        "max_event_time": _format_timestamp(max(event_times)),
        "min_ingest_time": _format_timestamp(min(ingest_times)),
        "max_ingest_time": _format_timestamp(max(ingest_times)),
        "groups": groups,
    }


def manifest_bytes(object_key: str, rows: list[dict[str, Any]]) -> bytes:
    return json.dumps(build_manifest(object_key, rows), ensure_ascii=False, indent=2).encode("utf-8")
