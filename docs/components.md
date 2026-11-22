# Компоненты и ответственность

| Компонент | Вход | Выход | Ответственность |
|---|---|---|---|
| `bybit-collector` | Bybit WebSocket | `market.trades`, `market.orderbook` | Сбор Bybit, нормализация, публикация |
| `okx-collector` | OKX WebSocket | `market.trades`, `market.orderbook` | Сбор OKX, нормализация, публикация |
| Kafka | События collectors | Kafka topics | Буфер, порядок внутри partition, offsets |
| `kafka-init` | Kafka broker | Kafka topics | Одноразовое создание `market.trades` и `market.orderbook` |
| `market-archiver` | Два Kafka topic | Parquet objects в MinIO | Raw-архивирование и подтверждение offsets |
| MinIO | Parquet objects | S3 API | Долговременное объектное хранение raw-слоя |
| Iceberg Catalog | Metadata commits | Iceberg tables | Snapshots, schema и partition metadata |
| Trino | Iceberg tables | SQL result sets | SQL-доступ к raw и staging слоям |
| dbt | Trino relations | Models and tests | Очистка, типизация, агрегации и проверки |
| ClickHouse | dbt marts | Analytics queries | Быстрые аналитические витрины |
| Airflow | DAG schedule | Jobs | Оркестрация dbt и REST-backfill |
| Telegram notifier | Alert condition | Telegram message | Уведомления о спреде |

## Запрещённое смешение ответственности

- Collector не пишет напрямую в ClickHouse.
- Kafka не выполняет бизнес-агрегации.
- Raw archiver не рассчитывает свечи или спред.
- dbt не получает данные напрямую из WebSocket.
- ClickHouse не является единственным raw-архивом.
