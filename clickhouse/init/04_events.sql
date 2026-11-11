-- Факт проходов через турникеты: локальная таблица шарда + Distributed-обёртка _all
-- для запросов. line_id заполняется словарём в приёмном MV (см. 06_kafka_mv.sql).
-- Ключ station_id в Distributed рабочий: приёмный MV пишет через эту обёртку,
-- поэтому все строки станции лежат на одном шарде (запросы с фильтром по станции
-- отсекают лишние шарды).

CREATE TABLE IF NOT EXISTS metro.turnstile_events ON CLUSTER metro_cluster (
    event_time DateTime64(3),
    event_date Date MATERIALIZED toDate(event_time),
    station_id UInt16,
    line_id UInt16 DEFAULT 0,
    turnstile_id UInt16,
    direction Enum8('in' = 1, 'out' = 2),
    train_id UInt32 DEFAULT 0,
    card_hash String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/turnstile_events', '{replica}')
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (station_id, event_time)
TTL toDateTime(event_time) + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS metro.turnstile_events_all ON CLUSTER metro_cluster
AS metro.turnstile_events
ENGINE = Distributed('metro_cluster', 'metro', 'turnstile_events', station_id);
