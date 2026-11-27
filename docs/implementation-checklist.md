# Чек-лист сверки реализации

Перед изменением кода проверяется каждый пункт. Если пункт ещё не реализован, он должен оставаться помеченным как `planned` в документации.

## Collectors

- [x] Bybit и OKX запускаются отдельными процессами.
- [x] Trades и orderbook имеют отдельные задачи обработки.
- [x] Для каждого источника есть reconnect и resubscribe.
- [x] Некорректные сообщения не останавливают collector.
- [x] Все события проходят единый data contract.
- [x] У каждого события формируется устойчивый `event_id`.

## Kafka

- [x] `market.trades` и `market.orderbook` создаются с заданными параметрами.
- [x] Определена стратегия Kafka key и partitioning.
- [ ] Определена политика retention.
- [ ] Ошибочные сообщения отправляются в DLQ.
- [x] Producer и consumer имеют явные настройки доставки.
- [x] MinIO и ClickHouse raw используют разные consumer groups.

## Raw archive

- [x] Archiver использует стабильный hash-based object key для повторной записи того же batch.
- [x] Flush выполняется по времени и имеет row limit как защиту буфера.
- [x] Offsets коммитятся только после успешной записи соответствующего sink.
- [ ] Повторная доставка не создаёт неконтролируемые дубликаты.
- [x] Raw partition path использует UTC date/hour, а exchange, event_type и symbol остаются колонками.

## Raw retention и очистка

- [ ] Настроить MinIO lifecycle policy.
- [ ] Зафиксировать срок хранения raw Parquet в MinIO.
- [ ] Добавить TTL для удаления объектов старше заданного срока.
- [ ] Добавить безопасное удаление старой partition-схемы `exchange/event_type/symbol/date/hour` после проверки миграции.
- [x] Определить контроль количества и размера Parquet-файлов.

## Gap recovery и orderbook

- [x] Фиксируется окно разрыва trade-WebSocket.
- [x] Пропущенные trades дозагружаются через публичный REST API Bybit и OKX.
- [x] Backfill публикует события в Kafka с тем же контрактом и `event_id`.
- [x] OKX backfill поддерживает pagination через `history-trades`.
- [x] Bybit backfill читает официальный daily public archive для завершённых UTC-дней.
- [x] Полное покрытие не маскируется под успех: ведётся `ops.trade_gap_coverage` со статусами `STARTED`, `RECOVERED`, `PARTIAL`, `UNRECOVERED`.
- [ ] Airflow повторно обрабатывает `PARTIAL` gaps после публикации отложенного Bybit archive.
- [ ] Проверяется непрерывность sequence стакана.
- [ ] При gap выполняется повторный snapshot.

## Analytical layer

- [x] ClickHouse raw writer получает полный поток через отдельную consumer group.
- [x] dbt-модели читают ClickHouse raw и имеют явные источники и зависимости.
- [x] dbt-тесты проверяют not-null и unique для текущих аналитических моделей.
- [x] dbt-тесты сверяют свечи с trades каждой биржи и spread между биржами.
- [x] ClickHouse разделяет raw-таблицы и подготовленные analytics-витрины.
- [ ] Airflow запускает dbt, тесты и reconciliation по расписанию.
- [ ] Telegram получает только подтверждённые alert-события.

## Definition of done

Компонент считается реализованным только после одновременного выполнения трёх условий:

1. Есть код или конфигурация.
2. Есть тест или воспроизводимая проверка.
3. Документация помечает компонент как реализованный.

## ClickHouse raw

- [x] `raw.market_events` использует `ReplacingMergeTree(ingest_time)`.
- [x] Raw writer сбрасывает буфер каждые 1 секунду или 1000 событий.
- [x] TTL ClickHouse raw зафиксирован на 7 дней.
- [x] Добавить manifest-файлы для полной сверки MinIO и ClickHouse.
- [x] Добавить служебную таблицу результатов сверки.
- [x] Сверка сравнивает count, unique `event_id` и digest после дедупликации ClickHouse raw.
- [ ] Запускать reconciliation по расписанию через Airflow.
- [ ] Добавить recovery отсутствующих событий из MinIO.
