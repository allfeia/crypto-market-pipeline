# Crypto Market Pipeline

Потоковый ETL/ELT-пайплайн рыночных данных Bybit и OKX. Проект собирает публичные сделки и обновления стакана, нормализует форматы бирж, передаёт события через Kafka и архивирует их в почасовые Parquet-партиции.

## Архитектура

```text
Bybit / OKX WebSocket
        ↓
Python collectors
        ↓
Kafka
       ↙   ↘
MinIO raw  ClickHouse raw
                ↓
           dbt-clickhouse
                ↓
       ClickHouse analytics marts
```

Sequence-диаграмма находится в [docs/sequence-diagram.md](docs/sequence-diagram.md), deployment diagram — в [docs/deployment.md](docs/deployment.md).

## Реализовано

- Асинхронные WebSocket-клиенты Bybit и OKX для spot BTC/USDT.
- Сбор trades и order book updates.
- Переподключение с exponential backoff.
- Нормализация сообщений в единую схему.
- Детерминированный UUID5 `event_id` для идентификации и дедупликации в текущем буфере archiver.
- JSONL sink для локальной проверки.
- Kafka sink с topics `market.trades` и `market.orderbook`.
- Kafka consumer для записи событий в Parquet. Partition path включает только UTC date и hour. Поля exchange, event_type и symbol хранятся внутри Parquet.
- Две независимые Kafka consumer groups для MinIO и ClickHouse raw.
- Kafka, MinIO, ClickHouse и два collector-контейнера в Docker Compose.
- ClickHouse raw writer и dbt-модели свечей и межбиржевого spread.

## Установка

```bash
.venv/bin/python -m pip install .
.venv/bin/python -m pip install '.[dev]'
```

## Сбор событий в Kafka

Запустить два независимых процесса, по одному на биржу:

```bash
.venv/bin/bybit-collector \
  --sink kafka \
  --symbol BTCUSDT \
  --kafka-bootstrap-servers localhost:9092
```

```bash
.venv/bin/okx-collector \
  --sink kafka \
  --symbol BTCUSDT \
  --kafka-bootstrap-servers localhost:9092
```

## Архивация Kafka-событий

Запись raw-событий в MinIO:

```bash
.venv/bin/market-archiver \
  --sink minio \
  --minio-endpoint localhost:9000 \
  --minio-bucket market-raw \
  --bootstrap-servers localhost:9092 \
  --flush-interval-seconds 60 \
  --flush-rows 10000
```

Пример partition path:

```text
date=2026-11-22/hour=14/part-<batch-hash>.parquet
```

## Локальная инфраструктура

```bash
docker compose up -d --build kafka minio bybit-collector okx-collector
```

Kafka доступен на `localhost:9092`. Внутри Compose collectors подключаются к Kafka по адресу `kafka:29092`. MinIO API доступен на `http://localhost:9000`, web-консоль — на `http://localhost:9001`.

Для локального Compose используется pinned community-образ `alpine/minio`, потому что официальные Community-образы MinIO недоступны из используемых registry.

Логи collectors:

```bash
docker compose logs -f bybit-collector okx-collector
```

## ClickHouse и dbt

ClickHouse хранит две группы таблиц. `raw.market_events` получает полный поток через отдельную Kafka consumer group, а dbt читает raw-слой и записывает свечи и межбиржевой spread в analytics-таблицы.

Запустить ClickHouse и raw writer вместе с Kafka:

```bash
docker compose up -d --build kafka minio clickhouse bybit-collector okx-collector clickhouse-raw-writer
```

Для запуска dbt установи зависимости из `analytics/requirements.txt`, скопируй `analytics/profiles.yml.example` в `~/.dbt/profiles.yml`, затем выполни:

```bash
cd analytics
dbt run
dbt test
```

Raw ClickHouse хранится 7 дней. MinIO остаётся долговременным raw-архивом.

## Тесты

```bash
.venv/bin/pytest -q
```

Тесты проверяют UUID5, нормализацию Bybit и OKX, почасовую partition-схему и формирование Parquet batch-а.
