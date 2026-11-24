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
- MinIO хранит долговременный raw-архив.
- ClickHouse разделяет raw-таблицы и аналитические витрины.
- dbt читает `ClickHouse raw` и записывает модели в `ClickHouse analytics`.
- Airflow запускает dbt и проверки по расписанию после подключения этих компонентов.
- Telegram принимает алерты о рассчитанных арбитражных окнах.

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

Реализованы collectors, Kafka sink, Kafka archiver, Parquet, MinIO sink, ClickHouse raw writer и базовые dbt-модели. Airflow, REST-backfill, Telegram, production-сверка и мониторинг пока являются следующими слоями реализации.
