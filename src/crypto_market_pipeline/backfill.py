from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import NormalizedEvent, deterministic_gap_id
from .normalizers import normalize_bybit_message, normalize_okx_message

logger = logging.getLogger(__name__)
EventWriter = Callable[[list[NormalizedEvent]], Awaitable[None]]
GapReporter = Callable[["GapCoverage"], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class GapCoverage:
    gap_id: str
    exchange: str
    symbol: str
    requested_start: datetime
    requested_end: datetime
    source: str
    fetched_rows: int
    published_rows: int
    covered_start: datetime | None
    covered_end: datetime | None
    status: str
    attempts: int
    error: str = ""
    checked_at: datetime | None = None

    def as_row(self) -> tuple[object, ...]:
        return (
            uuid.UUID(self.gap_id),
            self.exchange,
            self.symbol,
            self.requested_start,
            self.requested_end,
            self.source,
            self.fetched_rows,
            self.published_rows,
            self.covered_start,
            self.covered_end,
            self.status,
            self.attempts,
            self.error,
            self.checked_at or datetime.now(timezone.utc),
        )


def _milliseconds(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1000)


def _fetch_json(url: str, params: dict[str, str]) -> dict:
    request = Request(f"{url}?{urlencode(params)}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def _inside_window(event: NormalizedEvent, start: datetime, end: datetime) -> bool:
    event_time = datetime.fromisoformat(event.event_time.replace("Z", "+00:00"))
    return _milliseconds(start) <= _milliseconds(event_time) <= _milliseconds(end)


def _event_time(event: NormalizedEvent) -> datetime:
    return datetime.fromisoformat(event.event_time.replace("Z", "+00:00"))


def _sort_events(events: Iterable[NormalizedEvent]) -> list[NormalizedEvent]:
    return sorted(events, key=lambda event: (_event_time(event), event.event_id))


def _deduplicate(events: Iterable[NormalizedEvent]) -> list[NormalizedEvent]:
    return _sort_events({event.event_id: event for event in events}.values())


def fetch_bybit_trades(symbol: str, start: datetime, end: datetime) -> list[NormalizedEvent]:
    response = _fetch_json(
        "https://api.bybit.com/v5/market/recent-trade",
        {"category": "spot", "symbol": symbol, "limit": "60"},
    )
    if response.get("retCode") != 0:
        raise RuntimeError(f"Bybit REST error: {response.get('retMsg', response)}")
    trades = [
        {
            "i": item.get("execId"),
            "T": item.get("time"),
            "s": item.get("symbol", symbol),
            "p": item.get("price"),
            "v": item.get("size"),
            "S": item.get("side"),
            "seq": item.get("seq"),
            "raw": item,
        }
        for item in response.get("result", {}).get("list", [])
    ]
    events = normalize_bybit_message(
        {"topic": f"publicTrade.{symbol}", "data": trades},
        symbol,
    )
    return _sort_events([event for event in events if _inside_window(event, start, end)])


fetch_bybit_recent_trades = fetch_bybit_trades


def _date_range(start: datetime, end: datetime) -> list[date]:
    current = start.astimezone(timezone.utc).date()
    last = end.astimezone(timezone.utc).date()
    result: list[date] = []
    while current <= last:
        result.append(current)
        current += timedelta(days=1)
    return result


def _bybit_archive_urls(symbol: str, day: date) -> tuple[str, str]:
    base = f"https://public.bybit.com/spot/{symbol}"
    return (
        f"{base}/{symbol}_{day.isoformat()}.csv.gz",
        f"{base}/{symbol}-{day.strftime('%Y-%m')}.csv.gz",
    )


def _read_archive_rows(url: str) -> list[dict[str, str]]:
    request = Request(url, headers={"Accept": "application/gzip"})
    with urlopen(request, timeout=30) as response:
        compressed = response.read()
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
        text = io.TextIOWrapper(archive, encoding="utf-8", newline="")
        reader = csv.DictReader(text)
        fields = set(reader.fieldnames or ())
        required = {"timestamp", "price", "side"}
        if not required.issubset(fields) or not ({"id", "trdMatchID"} & fields) or not ({"volume", "size"} & fields):
            raise ValueError(f"Bybit archive has unsupported columns: {sorted(fields)}")
        return list(reader)


def _archive_timestamp(value: str) -> datetime:
    timestamp = float(value)
    if timestamp < 100_000_000_000:
        timestamp *= 1000
    return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)


def _archive_event(row: dict[str, str], symbol: str) -> NormalizedEvent | None:
    exchange_event_id = row.get("id") or row.get("trdMatchID")
    timestamp = row.get("timestamp")
    price = row.get("price")
    quantity = row.get("volume") or row.get("size")
    side = row.get("side")
    if not all((exchange_event_id, timestamp, price, quantity, side)):
        return None
    return NormalizedEvent.create(
        exchange="bybit",
        market_type="spot",
        symbol=row.get("symbol") or symbol,
        event_type="trade",
        exchange_event_id=str(exchange_event_id),
        event_time=_archive_timestamp(timestamp),
        payload={
            "price": price,
            "quantity": quantity,
            "side": side.lower(),
            "raw": row,
        },
    )


def fetch_bybit_archive_trades(symbol: str, start: datetime, end: datetime) -> list[NormalizedEvent]:

    events: list[NormalizedEvent] = []
    archive_cache: dict[str, list[dict[str, str]]] = {}
    for day in _date_range(start, end):
        rows: list[dict[str, str]] | None = None
        for url in _bybit_archive_urls(symbol, day):
            if url in archive_cache:
                rows = archive_cache[url]
                break
            try:
                rows = _read_archive_rows(url)
                archive_cache[url] = rows
                break
            except HTTPError as error:
                if error.code != 404:
                    raise
        if rows is None:
            raise FileNotFoundError(f"Bybit archive is unavailable for UTC date {day.isoformat()}")
        events.extend(
            event
            for row in rows
            if (event := _archive_event(row, symbol)) is not None
            and _inside_window(event, start, end)
        )
    return _deduplicate(events)


def fetch_okx_trades(symbol: str, start: datetime, end: datetime, max_pages: int = 1000) -> list[NormalizedEvent]:
    okx_symbol = symbol[:-4] + "-" + symbol[-4:] if symbol.endswith("USDT") else symbol
    params = {"instId": okx_symbol, "limit": "100", "type": "1"}
    all_events: list[NormalizedEvent] = []
    previous_after: str | None = None
    for _ in range(max_pages):
        response = _fetch_json("https://www.okx.com/api/v5/market/history-trades", params)
        if response.get("code") != "0":
            raise RuntimeError(f"OKX REST error: {response.get('msg', response)}")
        records = response.get("data", [])
        if not records:
            break
        all_events.extend(
            normalize_okx_message(
                {"arg": {"channel": "trades", "instId": okx_symbol}, "data": records},
                symbol,
            )
        )
        oldest_time = min(int(record["ts"]) for record in records if record.get("ts") is not None)
        oldest_trade_id = str(records[-1].get("tradeId", ""))
        if oldest_time <= _milliseconds(start) or not oldest_trade_id or oldest_trade_id == previous_after:
            break
        previous_after = oldest_trade_id
        params["after"] = oldest_trade_id
    return _sort_events([event for event in all_events if _inside_window(event, start, end)])


def _coverage_bounds(events: Iterable[NormalizedEvent]) -> tuple[datetime | None, datetime | None]:
    values = [_event_time(event) for event in events]
    return (min(values), max(values)) if values else (None, None)


async def _report(reporter: GapReporter | None, coverage: GapCoverage) -> None:
    if reporter is not None:
        await reporter(coverage)


async def recover_trade_gap(
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    write: EventWriter,
    report: GapReporter | None = None,
) -> int:

    gap_id = deterministic_gap_id(exchange, symbol, start, end)
    await _report(
        report,
        GapCoverage(
            gap_id=gap_id,
            exchange=exchange,
            symbol=symbol,
            requested_start=start,
            requested_end=end,
            source="pending",
            fetched_rows=0,
            published_rows=0,
            covered_start=None,
            covered_end=None,
            status="STARTED",
            attempts=0,
            checked_at=datetime.now(timezone.utc),
        ),
    )

    events: list[NormalizedEvent] = []
    source = ""
    complete = False
    error = ""
    attempts = 0
    for attempts in range(1, 4):
        try:
            if exchange == "bybit":
                try:
                    events = await asyncio.to_thread(fetch_bybit_archive_trades, symbol, start, end)
                    source = "bybit_public_archive"
                    complete = True
                except FileNotFoundError as archive_error:
                    logger.warning("Bybit archive is not ready for %s: %s", gap_id, archive_error)
                    events = await asyncio.to_thread(fetch_bybit_trades, symbol, start, end)
                    source = "bybit_recent_trade"
                    complete = False
                    error = str(archive_error)
            else:
                events = await asyncio.to_thread(fetch_okx_trades, symbol, start, end)
                source = "okx_history_trades"
                covered_start, _ = _coverage_bounds(events)
                complete = covered_start is not None and covered_start <= start
                if not complete:
                    error = "OKX history pagination did not reach the requested gap start"
            break
        except Exception as fetch_error:
            error = str(fetch_error)
            if attempts == 3:
                await _report(
                    report,
                    GapCoverage(
                        gap_id=gap_id,
                        exchange=exchange,
                        symbol=symbol,
                        requested_start=start,
                        requested_end=end,
                        source=source or "unknown",
                        fetched_rows=0,
                        published_rows=0,
                        covered_start=None,
                        covered_end=None,
                        status="UNRECOVERED",
                        attempts=attempts,
                        error=error,
                        checked_at=datetime.now(timezone.utc),
                    ),
                )
                logger.exception("Trade gap %s could not be fetched", gap_id)
                return 0
            delay = 2 ** (attempts - 1)
            logger.warning("Trade gap %s fetch failed, retrying in %s seconds", gap_id, delay)
            await asyncio.sleep(delay)

    unique_events = _deduplicate(events)
    published_rows = 0
    if unique_events:
        try:
            await write(unique_events)
            published_rows = len(unique_events)
        except Exception as publish_error:
            error = str(publish_error)
            await _report(
                report,
                GapCoverage(
                    gap_id=gap_id,
                    exchange=exchange,
                    symbol=symbol,
                    requested_start=start,
                    requested_end=end,
                    source=source,
                    fetched_rows=len(unique_events),
                    published_rows=0,
                    covered_start=None,
                    covered_end=None,
                    status="UNRECOVERED",
                    attempts=attempts,
                    error=error,
                    checked_at=datetime.now(timezone.utc),
                ),
            )
            raise

    covered_start, covered_end = _coverage_bounds(unique_events)
    status = "RECOVERED" if complete else "PARTIAL"
    await _report(
        report,
        GapCoverage(
            gap_id=gap_id,
            exchange=exchange,
            symbol=symbol,
            requested_start=start,
            requested_end=end,
            source=source,
            fetched_rows=len(unique_events),
            published_rows=published_rows,
            covered_start=covered_start,
            covered_end=covered_end,
            status=status,
            attempts=attempts,
            error=error,
            checked_at=datetime.now(timezone.utc),
        ),
    )
    if status == "RECOVERED":
        logger.info("Trade gap %s fully recovered from %s with %s event(s)", gap_id, source, published_rows)
    else:
        logger.error("Trade gap %s remains PARTIAL after %s event(s): %s", gap_id, published_rows, error)
    return published_rows
