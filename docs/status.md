# Статус реализации

## Реализовано

- Отдельные команды `bybit-collector` и `okx-collector`.
- WebSocket-подписки на trades и orderbook.
- Переподключение с exponential backoff.
- Нормализация сообщений в общую схему.
- UUID5 `event_id`.
- Kafka topics `market.trades` и `market.orderbook`.
- Автоматическое создание Kafka topics через одноразовый `kafka-init` service.
- Один MinIO raw archiver, читающий оба topic.
- Raw Parquet partitioning выполняется только по UTC date и hour. `exchange`, `event_type` и `symbol` хранятся колонками.
- MinIO sink.
- Docker Compose services для Kafka, MinIO, Bybit collector и OKX collector.
- Базовые unit-тесты и CI.
- ClickHouse raw writer с consumer group `market-raw-clickhouse`.
- ClickHouse raw schema на `ReplacingMergeTree(ingest_time)` с TTL 7 дней.
- dbt staging и аналитические модели свечей и межбиржевого spread.
- 18 dbt-тестов not-null, unique и сверки аналитических моделей проходят на чистом локальном raw-потоке.
- Manifest-файлы для новых MinIO Parquet-part и таблица `ops.raw_reconciliation`.
- CLI `market-reconcile` для асинхронной сверки MinIO и ClickHouse raw.
- Интеграционно проверены статусы `MATCH`, `MISSING_IN_CLICKHOUSE` и `EXTRA_IN_CLICKHOUSE` на контролируемом Kafka backlog.
- Trade REST-backfill для Bybit и OKX после reconnect WebSocket с журналом покрытия gap.
- Bybit public archive fallback для завершённых UTC-дней и статусы `STARTED`, `RECOVERED`, `PARTIAL`, `UNRECOVERED` в `ops.trade_gap_coverage`.

## Частично реализовано

- Archiver удаляет повторяющиеся `event_id` в текущем буфере и использует стабильный hash имени для повторной записи того же batch. Долговременная дедупликация после рестарта ещё не реализована.
- Для текущего UTC-дня Bybit recent-trade публикует быстрый fallback, но он остаётся `PARTIAL` до появления authoritative daily archive. Для OKX статус `RECOVERED` возможен только если `history-trades` pagination достигла начала gap.
- Orderbook-сообщения архивируются, но непрерывное состояние стакана не восстанавливается.
- Flush выполняется по временным интервалам, а `flush_rows` используется как верхний лимит буфера.

## Не реализовано

- Production-проверка ClickHouse raw writer и масштабирование.
- Production-проверка dbt-сверок на длительном потоке и при late events.
- Production-проверка reconciliation на длительном потоке и при расхождениях.
- Airflow DAGs.
- Telegram alerts.
- DLQ для некорректных Kafka-сообщений.
- Production deployment и мониторинг.
- Airflow-расписание reconciliation.
- Recovery отсутствующих ClickHouse raw-событий из MinIO.
- Автоматический запуск повторной обработки `PARTIAL` gaps через Airflow.

Документация не должна считать планируемые компоненты реализованными, пока они не появились в коде, конфигурации и тестах.
