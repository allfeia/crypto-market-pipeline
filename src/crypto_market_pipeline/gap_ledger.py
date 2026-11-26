from __future__ import annotations

import asyncio
from typing import Any

from .backfill import GapCoverage


COLUMNS = (
    "gap_id",
    "exchange",
    "symbol",
    "requested_start",
    "requested_end",
    "source",
    "fetched_rows",
    "published_rows",
    "covered_start",
    "covered_end",
    "status",
    "attempts",
    "error",
    "checked_at",
)


class ClickHouseGapLedger:

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client: Any | None = None

    async def start(self) -> None:
        import clickhouse_connect

        self.client = await asyncio.to_thread(
            clickhouse_connect.get_client,
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database="ops",
        )

    async def record(self, coverage: GapCoverage) -> None:
        if self.client is None:
            raise RuntimeError("ClickHouseGapLedger.start() must be called before record()")
        for attempt in range(3):
            try:
                await asyncio.to_thread(
                    self.client.insert,
                    "ops.trade_gap_coverage",
                    [coverage.as_row()],
                    column_names=list(COLUMNS),
                )
                return
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2**attempt)

    async def close(self) -> None:
        if self.client is not None:
            await asyncio.to_thread(self.client.close)
