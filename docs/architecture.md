# Архитектура Crypto Market Pipeline

## Назначение

Пайплайн собирает публичные сделки и обновления стакана Bybit и OKX, сохраняет raw normalized events и подготавливает данные для аналитических витрин.

## Целевая схема

```text
Bybit collector                      OKX collector
├── trades task                      ├── trades task
└── orderbook task                   └── orderbook task
          │                                    │
          └────────────── Kafka ──────────────┘
                         │
                   raw archiver
                         │
                  MinIO + Parquet
                         │
                  Iceberg Catalog
                         │
                       Trino
                         │
                        dbt
                         │
                 ClickHouse marts
```

## Границы компонентов

- Collector отвечает за WebSocket, reconnect, нормализацию и публикацию в Kafka.
- Kafka отвечает за буферизацию, порядок внутри partition и повторную доставку.
- Raw archiver читает Kafka и записывает normalized events в MinIO в формате Parquet.
- Iceberg Catalog управляет таблицами, snapshot-ами и схемой raw-слоя.
- Trino читает Iceberg-таблицы через SQL.
- dbt выполняет staging, intermediate и mart-модели.
- ClickHouse хранит подготовленные аналитические витрины.
- Airflow запускает dbt и REST-backfill по расписанию.
- Telegram принимает алерты о рассчитанных арбитражных окнах.

## Зафиксированные решения

1. У каждой биржи отдельный collector-процесс.
2. Внутри collector trades и orderbook являются отдельными задачами или потоками обработки.
3. Raw-данные отделены от аналитических витрин.
4. MinIO используется как объектное raw-хранилище.
5. Trino используется как SQL-движок для MinIO и Iceberg.
6. Spark в текущий проект не входит.
7. dbt не подключается напрямую к MinIO. Он выполняется через Trino.
8. ClickHouse используется для очищенных и агрегированных витрин.

## Текущее состояние

Реализованы collectors, Kafka sink, Kafka archiver, Parquet и MinIO sink. Iceberg, Trino, dbt, ClickHouse, Airflow, REST-backfill и Telegram пока являются следующими слоями целевой архитектуры.
