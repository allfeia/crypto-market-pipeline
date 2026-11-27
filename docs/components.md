# Компоненты и ответственность

| Компонент | Вход | Выход | Ответственность |
|---|---|---|---|
| `bybit-collector` | Bybit WebSocket | `market.trades`, `market.orderbook` | Сбор Bybit, нормализация, публикация |
| `okx-collector` | OKX WebSocket | `market.trades`, `market.orderbook` | Сбор OKX, нормализация, публикация |
| Kafka | События collectors | Kafka topics | Буфер, порядок внутри partition, offsets |
| `kafka-init` | Kafka broker | Kafka topics | Одноразовое создание `market.trades` и `market.orderbook` |
| `market-raw-minio` | Два Kafka topic | Parquet objects в MinIO | Raw-архивирование и подтверждение offsets после записи в MinIO |
| `market-raw-clickhouse` | Два Kafka topic | `ClickHouse raw.events` | Raw-копия для быстрых запросов и подтверждение offsets после вставки |
| MinIO | Parquet objects | S3 API | Долговременное объектное хранение raw-слоя |
| ClickHouse raw | События ClickHouse consumer | Raw SQL tables | Оперативный raw-слой без бизнес-агрегаций |
| dbt | ClickHouse raw relations | ClickHouse analytics tables | Очистка, типизация, агрегации и проверки |
| ClickHouse analytics | dbt models | Analytics queries | Быстрые аналитические витрины |
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

- Collector не пишет напрямую в ClickHouse.
- Kafka не выполняет бизнес-агрегации.
- Raw sinks не рассчитывают свечи или спред.
- dbt не получает данные напрямую из WebSocket.
- dbt не изменяет `ClickHouse raw.events`.
- ClickHouse analytics не смешивается с raw-таблицами.
