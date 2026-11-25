from __future__ import annotations

import io
import json
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as parquet

from .models import NormalizedEvent
from .manifest import manifest_bytes, manifest_key


class ArchiveSink(Protocol):
    def write_part(self, object_key: str, rows: list[dict[str, Any]]) -> None: ...


@dataclass(frozen=True, slots=True)
class HourPartition:
    date: str
    hour: str

    @property
    def prefix(self) -> str:
        return f"date={self.date}/hour={self.hour}"


def event_partition(event: NormalizedEvent) -> HourPartition:
    timestamp = datetime.fromisoformat(event.event_time.replace("Z", "+00:00")).astimezone(timezone.utc)
    return HourPartition(
        date=timestamp.strftime("%Y-%m-%d"),
        hour=timestamp.strftime("%H"),
    )


def event_to_row(event: NormalizedEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "exchange": event.exchange,
        "market_type": event.market_type,
        "symbol": event.symbol,
        "event_type": event.event_type,
        "exchange_event_id": event.exchange_event_id,
        "event_time": event.event_time,
        "ingest_time": event.ingest_time,
        "payload_json": json.dumps(event.payload, ensure_ascii=False, separators=(",", ":")),
    }


def rows_to_parquet(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buffer = io.BytesIO()
    parquet.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


class MinioParquetSink:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool = False) -> None:
        from minio import Minio

        host = endpoint.removeprefix("http://").removeprefix("https://")
        self.client = Minio(host, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def write_part(self, object_key: str, rows: list[dict[str, Any]]) -> None:
        payload = rows_to_parquet(rows)
        self.client.put_object(
            self.bucket,
            object_key,
            io.BytesIO(payload),
            length=len(payload),
            content_type="application/vnd.apache.parquet",
        )
        manifest = manifest_bytes(object_key, rows)
        self.client.put_object(
            self.bucket,
            manifest_key(object_key),
            io.BytesIO(manifest),
            length=len(manifest),
            content_type="application/json",
        )


class HourlyArchive:
    def __init__(self, sink: ArchiveSink) -> None:
        self.sink = sink
        self._batches: dict[HourPartition, list[dict[str, Any]]] = defaultdict(list)
        self._seen_event_ids: set[str] = set()

    @property
    def pending_rows(self) -> int:
        return sum(len(rows) for rows in self._batches.values())

    def add(self, event: NormalizedEvent) -> bool:
        if event.event_id in self._seen_event_ids:
            return False

        self._seen_event_ids.add(event.event_id)
        self._batches[event_partition(event)].append(event_to_row(event))
        return True

    @staticmethod
    def _object_key(partition: HourPartition, rows: list[dict[str, Any]]) -> str:
        identity = "\n".join(
            sorted(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) for row in rows)
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        return f"{partition.prefix}/part-{digest}.parquet"

    def flush(self) -> int:
        written = 0
        for partition, rows in list(self._batches.items()):
            object_key = self._object_key(partition, rows)
            self.sink.write_part(object_key, rows)
            written += len(rows)
        self._batches.clear()
        self._seen_event_ids.clear()
        return written
