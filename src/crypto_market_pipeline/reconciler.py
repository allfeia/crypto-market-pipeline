from __future__ import annotations

import argparse
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    exchange: str
    symbol: str
    event_type: str
    object_key: str
    expected_rows: int
    actual_rows: int
    expected_unique_event_ids: int
    actual_unique_event_ids: int
    expected_event_id_digest: str
    actual_event_id_digest: str
    status: str
    checked_at: datetime


def digest_event_ids(event_ids: list[str]) -> str:
    identity = "\n".join(sorted(event_ids))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def xor_event_ids(event_ids: list[str]) -> str:
    checksum = 0
    for event_id in event_ids:
        checksum ^= int(hashlib.sha256(event_id.encode("utf-8")).hexdigest(), 16)
    return f"{checksum:064x}"


def classify_result(
    *,
    expected_rows: int,
    actual_rows: int,
    expected_unique_event_ids: int,
    actual_unique_event_ids: int,
    expected_digest: str,
    actual_digest: str,
) -> str:
    if actual_unique_event_ids < expected_unique_event_ids:
        return "MISSING_IN_CLICKHOUSE"
    if actual_unique_event_ids > expected_unique_event_ids:
        return "EXTRA_IN_CLICKHOUSE"
    if actual_rows > actual_unique_event_ids:
        return "DUPLICATES_IN_CLICKHOUSE"
    if actual_rows != expected_rows or (expected_digest and actual_digest != expected_digest):
        return "METADATA_MISMATCH"
    return "MATCH"


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _datetime_literal(value: str) -> str:
    timestamp = parse_timestamp(value).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"toDateTime64({_quote(timestamp)}, 3, 'UTC')"


def query_clickhouse_group(client: Any, group: dict[str, Any]) -> tuple[int, int, datetime | None, datetime | None, str]:
    query = f"""
        SELECT
            count() AS row_count,
            min(event_time) AS min_event_time,
            max(event_time) AS max_event_time,
            groupArray(toString(event_id)) AS event_ids
        FROM (
            SELECT
                event_id,
                argMax(event_time, ingest_time) AS event_time,
                argMax(ingest_time, ingest_time) AS latest_ingest_time
            FROM raw.market_events AS source_events
            WHERE source_events.exchange = {_quote(group['exchange'])}
              AND source_events.symbol = {_quote(group['symbol'])}
              AND source_events.event_type = {_quote(group['event_type'])}
              AND source_events.event_time >= {_datetime_literal(group['hour_start'])}
              AND source_events.event_time < {_datetime_literal(group['hour_end'])}
            GROUP BY source_events.event_id
        )
    """
    row = client.query(query).result_rows[0]
    event_ids = [str(event_id) for event_id in row[3]]
    return int(row[0]), len(event_ids), row[1], row[2], xor_event_ids(event_ids)


def manifest_groups(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for group in manifest.get("groups", []):
        prefix = manifest["object_key"].split("/part-", 1)[0]
        date_value = prefix.split("date=", 1)[1].split("/", 1)[0]
        hour_value = prefix.split("hour=", 1)[1]
        groups.append(
            {
                **group,
                "object_key": manifest["object_key"],
                "hour_start": f"{date_value}T{hour_value}:00:00.000Z",
                "hour_end": f"{date_value}T{hour_value}:59:59.999Z",
            }
        )
    return groups


def aggregate_groups(manifests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for manifest in manifests:
        for group in manifest_groups(manifest):
            prefix = group["object_key"].split("/part-", 1)[0]
            key = (prefix, group["exchange"], group["symbol"], group["event_type"])
            item = aggregated.setdefault(
                key,
                {**group, "object_key": prefix, "row_count": 0, "unique_event_ids": 0, "event_id_xor": 0, "event_id_digest": "", "has_event_id_xor": True},
            )
            item["row_count"] += int(group["row_count"])
            item["unique_event_ids"] += int(group["unique_event_ids"])
            if group.get("event_id_xor"):
                item["event_id_xor"] ^= int(group["event_id_xor"], 16)
            else:
                item["has_event_id_xor"] = False
    for item in aggregated.values():
        checksum = item.pop("event_id_xor")
        if item.pop("has_event_id_xor"):
            item["event_id_digest"] = f"{checksum:064x}"
    return list(aggregated.values())


def iter_manifests(minio_client: Any, bucket: str) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for item in minio_client.list_objects(bucket, recursive=True):
        if not item.object_name.endswith(".manifest.json"):
            continue
        response = minio_client.get_object(bucket, item.object_name)
        try:
            manifests.append(json.loads(response.read()))
        finally:
            response.close()
            response.release_conn()
    return manifests


def reconcile(args: argparse.Namespace) -> list[ReconciliationResult]:
    from minio import Minio
    import clickhouse_connect

    minio = Minio(
        args.minio_endpoint.removeprefix("http://").removeprefix("https://"),
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.minio_secure,
    )
    clickhouse = clickhouse_connect.get_client(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        username=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    checked_at = datetime.now(timezone.utc)
    results: list[ReconciliationResult] = []
    manifests = iter_manifests(minio, args.minio_bucket)
    for group in aggregate_groups(manifests):
            actual_rows, actual_unique, _, _, actual_digest = query_clickhouse_group(clickhouse, group)
            results.append(
                ReconciliationResult(
                    exchange=group["exchange"],
                    symbol=group["symbol"],
                    event_type=group["event_type"],
                    object_key=group["object_key"],
                    expected_rows=int(group["row_count"]),
                    actual_rows=actual_rows,
                    expected_unique_event_ids=int(group["unique_event_ids"]),
                    actual_unique_event_ids=actual_unique,
                    expected_event_id_digest=group["event_id_digest"],
                    actual_event_id_digest=actual_digest,
                    status=classify_result(
                        expected_rows=int(group["row_count"]),
                        actual_rows=actual_rows,
                        expected_unique_event_ids=int(group["unique_event_ids"]),
                        actual_unique_event_ids=actual_unique,
                        expected_digest=group["event_id_digest"],
                        actual_digest=actual_digest,
                    ),
                    checked_at=checked_at,
                )
            )
    insert_results(clickhouse, results)
    clickhouse.close()
    return results


def insert_results(client: Any, results: list[ReconciliationResult]) -> None:
    if not results:
        logger.info("No manifest files found")
        return
    rows = [
        (
            result.checked_at,
            result.object_key,
            result.exchange,
            result.symbol,
            result.event_type,
            result.expected_rows,
            result.actual_rows,
            result.expected_unique_event_ids,
            result.actual_unique_event_ids,
            result.expected_event_id_digest,
            result.actual_event_id_digest,
            result.status,
        )
        for result in results
    ]
    client.insert(
        "ops.raw_reconciliation",
        rows,
        column_names=[
            "checked_at",
            "object_key",
            "exchange",
            "symbol",
            "event_type",
            "expected_rows",
            "actual_rows",
            "expected_unique_event_ids",
            "actual_unique_event_ids",
            "expected_event_id_digest",
            "actual_event_id_digest",
            "status",
        ],
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile MinIO raw manifests with ClickHouse raw")
    parser.add_argument("--minio-endpoint", default="localhost:9000")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--minio-bucket", default="market-raw")
    parser.add_argument("--minio-secure", action="store_true")
    parser.add_argument("--clickhouse-host", default="localhost")
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-user", default="pipeline")
    parser.add_argument("--clickhouse-password", default="pipeline")
    return parser


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    results = reconcile(build_parser().parse_args())
    for result in results:
        logger.info(
            "%s %s/%s/%s rows=%s/%s unique=%s/%s object=%s",
            result.status,
            result.exchange,
            result.symbol,
            result.event_type,
            result.expected_rows,
            result.actual_rows,
            result.expected_unique_event_ids,
            result.actual_unique_event_ids,
            result.object_key,
        )


if __name__ == "__main__":
    run()
