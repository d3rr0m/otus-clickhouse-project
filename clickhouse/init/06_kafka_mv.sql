-- Приём из Kafka: единая точка входа на ch-shard1-r1.
-- Файл последний в цепочке init-скриптов: kafka-таблица создаётся, когда вся остальная
-- схема уже применена на всех узлах, — консьюмер стартует не раньше, чем приёмный MV
-- и агрегационные MV гарантированно живут на обеих первых репликах (ON CLUSTER ждёт
-- хосты, а DDLWorker вторым планом в момент потока данных создавать триггеры поздно).
--
-- Группа clickhouse_ingest из одного участника читает все 6 партиций топика, каждая
-- строка потребляется ровно один раз. MV по ходу вставки тянет line_id из словаря
-- станций (denormalize) и пишет в Distributed turnstile_events_all; та раскладывает
-- строки по шардам по station_id (свой шард — фоновая вставка, чужой — удалённая).
-- CREATE без ON CLUSTER: файл исполняется только на ch-shard1-r1 (туда примонтирован
-- initdb, там же его запускает make reinit), объекты живут локально на входе.
-- Компромисс: единая точка приёма (упал ch-shard1-r1 — встал ввод, данные на узлах
-- остаются). При росте объёмов вход масштабируется возвратом к ON CLUSTER
-- kafka_initiators по консьюмеру на шард; тогда же TO у MV переключается на локальную
-- turnstile_events своего шарда.

DROP TABLE IF EXISTS metro.mv_kafka_events ON CLUSTER metro_cluster;
DROP TABLE IF EXISTS metro.kafka_events ON CLUSTER metro_cluster;

CREATE TABLE IF NOT EXISTS metro.kafka_events (
    event_time DateTime64(3),
    station_id UInt16,
    turnstile_id UInt16,
    direction Enum8('in' = 1, 'out' = 2),
    train_id UInt32,
    card_hash String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'metro.turnstile.events',
    kafka_group_name = 'clickhouse_ingest',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536;

-- POPULATE тут не нужен: источник — виртуальная kafka-таблица, история не читается,
-- стрим идёт с закоммиченных офсетов группы.

CREATE MATERIALIZED VIEW IF NOT EXISTS metro.mv_kafka_events
TO metro.turnstile_events_all AS
SELECT
    event_time,
    station_id,
    dictGet('metro.dict_stations', 'line_id', station_id) AS line_id,
    turnstile_id,
    direction,
    train_id,
    card_hash
FROM metro.kafka_events;
