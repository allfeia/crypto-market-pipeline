# Контракт нормализованного события

Каждое сообщение в Kafka содержит JSON-объект со следующими обязательными полями.

```json
{
  "event_id": "uuid5",
  "exchange": "bybit",
  "market_type": "spot",
  "symbol": "BTCUSDT",
  "event_type": "trade",
  "exchange_event_id": "exchange-specific-id",
  "event_time": "2026-11-22T03:00:00.000Z",
  "ingest_time": "2026-11-22T03:00:00.120Z",
  "payload": {}
}
```

## Правила

- `event_time` — время события на бирже в UTC.
- `ingest_time` — время получения события collector-ом в UTC.
- `symbol` — общий формат проекта, например `BTCUSDT`.
- Исходный символ биржи сохраняется в `payload.exchange_symbol`, если формат отличается.
- `event_type` принимает значения `trade` или `orderbook`.
- `exchange_event_id` содержит идентификатор сделки или обновления стакана, предоставленный биржей.
- `event_id` формируется детерминированно из биржи, рынка, символа, типа события и идентификатора биржи.
- UUID позволяет найти повторную доставку. Текущий archiver удаляет повторы внутри активного буфера, а полная дедупликация после рестарта относится к raw или staging-слою.
- Для orderbook повторная доставка определяется по идентификатору обновления биржи. Если биржа повторно использует sequence ID для другого сообщения, нужна отдельная проверка последовательности.
- При изменении структуры добавляется `schema_version` и обновляются тесты совместимости.

## Raw Parquet partitioning

Raw objects partitioned only by UTC date and hour:

```text
date=2026-11-22/hour=13/part-<batch-hash>.parquet
```

`exchange`, `event_type` and `symbol` are stored as columns in every Parquet row.

## Kafka topics

- `market.trades` — события сделок.
- `market.orderbook` — snapshot и update-сообщения стакана.
- `market.dlq` — планируемый topic для сообщений, которые не удалось распарсить или провалидировать. Сейчас DLQ не реализован.

## Ключ Kafka

Ключ сообщения имеет формат `exchange:symbol`. Он сохраняет порядок событий одного источника в одной partition. При добавлении большого количества символов нужно отдельно проверить распределение нагрузки.
