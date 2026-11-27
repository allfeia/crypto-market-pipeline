# Компоненты и ответственность

| Компонент | Вход | Выход | Ответственность |
|---|---|---|---|
| `bybit-collector` | Bybit WebSocket | `market.trades`, `market.orderbook` | Сбор Bybit, нормализация, публикация |
| `okx-collector` | OKX WebSocket | `market.trades`, `market.orderbook` | Сбор OKX, нормализация, публикация |
| Trade REST-backfill | Bybit/OKX public REST и Bybit public archive | `market.trades`, `ops.trade_gap_coverage` | Восстановление trade gaps с доказательством полного или частичного покрытия |
| Kafka | События collectors | Kafka topics | Буфер, порядок внутри partition, offsets |
| `kafka-init` | Kafka broker | Kafka topics | Одноразовое создание `market.trades` и `market.orderbook` |
| `market-raw-minio` | Два Kafka topic | Parquet objects в MinIO | Raw-архивирование и подтверждение offsets после записи в MinIO |
| `market-raw-clickhouse` | Два Kafka topic | `ClickHouse raw.events` | Raw-копия для быстрых запросов и подтверждение offsets после вставки |
| MinIO | Parquet objects | S3 API | Долговременное объектное хранение raw-слоя |
| ClickHouse raw | События ClickHouse consumer | Raw SQL tables | Оперативный raw-слой без бизнес-агрегаций |
| dbt | ClickHouse raw relations | ClickHouse analytics tables | Очистка, типизация, агрегации и проверки |
| ClickHouse analytics | dbt models | Analytics queries | Быстрые аналитические витрины |
| `market-reconcile` | MinIO manifests и ClickHouse raw | `ops.raw_reconciliation` | Асинхронная сверка двух raw-копий |
| ClickHouse ops | Reconciliation and gap coverage results | SQL audit queries | История сверки raw-копий и восстановления trade gaps |
| Airflow | DAG schedule | Jobs | Оркестрация dbt и проверок |
| Telegram notifier | Alert condition | Telegram message | Уведомления о спреде |

## Независимые Kafka consumer groups

Оба raw-sink получают каждое событие независимо:

```text
Kafka
├── consumer group market-raw-minio
│   └── market-raw-minio
└── consumer group market-raw-clickhouse
    └── market-raw-clickhouse
```

Использование одной consumer group распределило бы сообщения между sinks и не гарантировало бы запись каждого события в оба хранилища.

## Успешная доставка

Каждый sink коммитит offset только после успешной записи собственного события:

```text
Kafka → запись в MinIO → commit offset группы market-raw-minio
Kafka → вставка в ClickHouse → commit offset группы market-raw-clickhouse
```

Это обеспечивает целевую гарантию at-least-once. При временной недоступности sink offset не коммитится, и Kafka сохраняет сообщение для повторной обработки.

## Разделение ответственности

- Collector пишет только технический статус gap в `ops.trade_gap_coverage`.
- Trade REST-backfill публикует события только в Kafka и использует тот же контракт, что и WebSocket.
- Ledger gap recovery хранит в ClickHouse только состояние восстановления. Он не является вторым путём доставки рыночных событий.
- `RECOVERED` выставляется только после доказанного покрытия интервала. `PARTIAL` и `UNRECOVERED` требуют повторной обработки.
- В Compose collector зависит от healthcheck ClickHouse, чтобы не запускать поток без обязательного журнала покрытия. Это сознательная fail-closed зависимость для текущего этапа.
- Kafka не выполняет бизнес-агрегации.
- Raw sinks не рассчитывают свечи или спред.
- dbt не получает данные напрямую из WebSocket.
- dbt не изменяет `ClickHouse raw.events`.
- ClickHouse analytics не смешивается с raw-таблицами.
- Reconciliation не участвует в Kafka consumer groups и не блокирует raw ingestion.
