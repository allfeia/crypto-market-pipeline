from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
import json
import uuid


EVENT_NAMESPACE = uuid.UUID("3e8f0d72-9ed5-4f90-8c59-4a21d6d5f4ba")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_millis(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(slots=True)
class NormalizedEvent:
    event_id: str
    exchange: str
    market_type: str
    symbol: str
    event_type: str
    exchange_event_id: str
    event_time: str
    ingest_time: str
    payload: dict[str, Any]

    @classmethod
    def create(
        cls,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        event_type: str,
        exchange_event_id: str,
        event_time: datetime,
        payload: dict[str, Any],
    ) -> "NormalizedEvent":
        identity = "|".join((exchange, market_type, symbol, event_type, exchange_event_id))
        event_id = str(uuid.uuid5(EVENT_NAMESPACE, identity))
        return cls(
            event_id=event_id,
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            event_type=event_type,
            exchange_event_id=exchange_event_id,
            event_time=isoformat_millis(event_time),
            ingest_time=isoformat_millis(utc_now()),
            payload=payload,
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))
