from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Protocol

from .models import NormalizedEvent


class EventSink(Protocol):
    async def start(self) -> None: ...

    async def write(self, events: list[NormalizedEvent]) -> None: ...

    async def close(self) -> None: ...


class JsonlSink:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, asyncio.Lock] = {}

    async def start(self) -> None:
        return None

    async def write(self, events: list[NormalizedEvent]) -> None:
        for event in events:
            lock = self._locks.setdefault(event.exchange, asyncio.Lock())
            path = self.output_dir / f"{event.exchange}.jsonl"
            async with lock:
                with path.open("a", encoding="utf-8") as file:
                    file.write(event.to_json() + "\n")

    async def close(self) -> None:
        return None


class KafkaSink:
    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()

    async def write(self, events: list[NormalizedEvent]) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaSink.start() must be called before write()")

        for event in events:
            topic = "market.trades" if event.event_type == "trade" else "market.orderbook"
            key = f"{event.exchange}:{event.symbol}".encode("utf-8")
            await self._producer.send_and_wait(topic, event.to_json().encode("utf-8"), key=key)

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
