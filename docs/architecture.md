# Архитектура Crypto Market Pipeline

## Назначение

Пайплайн собирает публичные сделки и обновления стакана Bybit и OKX, сохраняет raw normalized events в MinIO и ClickHouse, а затем строит аналитические витрины в ClickHouse через dbt.

## Целевая схема

```text
Bybit collector                      OKX collector
├── trades task                      ├── trades task
└── orderbook task                   └── orderbook task
          │                                    │
          └────────────── Kafka ──────────────┘
                         │
              ┌──────────┴──────────┐
              │                     │
     market-raw-minio       market-raw-clickhouse
              │                     │
       MinIO raw Parquet     ClickHouse raw.events
                                    │
                              dbt-clickhouse
                                    │
                         ClickHouse analytics marts
```

## Границы компонентов

- Collector отвечает за WebSocket, reconnect, нормализацию и публикацию в Kafka.
- Kafka отвечает за буферизацию, порядок внутри partition и повторную доставку.
- `market-raw-minio` читает Kafka и записывает normalized events в MinIO в формате Parquet.
- `market-raw-clickhouse` независимо читает те же Kafka topics и записывает raw-события в ClickHouse.
- Trade REST-backfill после reconnect публикует восстановленные события в `market.trades` через тот же sink.
- Gap recovery записывает проверяемый статус в `ops.trade_gap_coverage`. Событие считается полностью восстановленным только при статусе `RECOVERED`.
- Для Bybit gap может быть немедленно заполнен recent-trade, но полное покрытие подтверждается официальным daily archive. До публикации архива статус остаётся `PARTIAL`.
- `market-reconcile` асинхронно сверяет новые MinIO manifests с логическими ClickHouse raw-событиями.
- MinIO хранит долговременный raw-архив.
- ClickHouse разделяет raw-таблицы и аналитические витрины.
- dbt читает `ClickHouse raw` и записывает модели в `ClickHouse analytics`.
- Airflow запускает dbt и проверки по расписанию после подключения этих компонентов.
- Telegram принимает алерты о рассчитанных арбитражных окнах.
- ClickHouse ops хранит результаты reconciliation и журнал покрытия trade gaps.

## Зафиксированные решения

1. У каждой биржи отдельный collector-процесс.
2. Внутри collector trades и orderbook являются отдельными задачами или потоками обработки.
3. Raw-данные отделены от аналитических витрин.
4. MinIO используется как долговременное объектное raw-хранилище.
5. MinIO и ClickHouse raw получают события через разные Kafka consumer groups.
6. Offset соответствующей consumer group коммитится только после успешной записи в её sink.
7. Целевая гарантия доставки — at-least-once.
8. dbt читает исходные данные из ClickHouse raw.
9. Raw-таблицы и аналитические витрины находятся в разных слоях ClickHouse.

## Текущее состояние

Реализованы collectors, Kafka sink, Kafka archiver, Parquet, MinIO sink, ClickHouse raw writer, проверяемый trade REST-backfill с Bybit public archive fallback, базовые dbt-модели и reconciliation manifests.
