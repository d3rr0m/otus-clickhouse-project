-- Агрегаты для дашбордов: поминутная нагрузка станций/линий и дельты по поездам
-- (+1 вход, -1 выход). SimpleAggregateFunction(sum) — инкрементально, без -State.
-- Target-таблицы шардируются через {shard} в пути ZK; MV создаются на ВСЕХ узлах
-- кластера, и вот почему: часть фактов попадает в шард не прямым INSERT, а фоновой
-- удалённой вставкой из Distributed-очереди входа, и эта очередь пишет на любую
-- реплику шарда. MV срабатывает только там, где physically был INSERT (репликация
-- частей триггер не вызывает) — оставь его только на r1, батчи, приехавшие на r2,
-- оседали бы в фактах без агрегатов (ровно это мы и измерили: ~40% фактов второй
-- реплики не попали в сверку). На любой реплике свой INSERT -> свой MV -> один счёт.
-- POPULATE намеренно не ставим: на время фонового скана MV игнорирует параллельные
-- вставки — на потоковом пайплайне это вторая гарантированная дыра. Пересборка слоя:
-- 00_drop_all + цепочка заново с остановленным генератором, либо точечно: truncate
-- целей, пересоздать MV и один раз прогнать
-- INSERT INTO ..._local SELECT ... FROM metro.turnstile_events GROUP BY ...
-- на остановленных потоках. Дроп объектов — в 00_drop_all.sql.

-- == Загрузка станций по минутам ==
CREATE TABLE IF NOT EXISTS metro.station_load_1m_local ON CLUSTER metro_cluster (
    minute DateTime,
    station_id UInt16,
    direction Enum8('in' = 1, 'out' = 2),
    passengers SimpleAggregateFunction(sum, UInt64)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/station_load_1m', '{replica}')
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute, station_id, direction)
-- dedup на целях агрегатов выключен (окно 0): атомарность факта и MV в одном INSERT
-- даёт ровно один вызов на блок, а оконные VIEW-токены удалённых Distributed-вставок
-- (source-id пустой) только мешают — при одинаковых размерах блоков они коллидируют.
SETTINGS replicated_deduplication_window = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS metro.mv_station_load_1m ON CLUSTER metro_cluster
TO metro.station_load_1m_local AS
SELECT
    toStartOfMinute(event_time) AS minute,
    station_id,
    direction,
    count() AS passengers
FROM metro.turnstile_events
GROUP BY minute, station_id, direction;

CREATE TABLE IF NOT EXISTS metro.station_load_1m ON CLUSTER metro_cluster
AS metro.station_load_1m_local
ENGINE = Distributed('metro_cluster', 'metro', 'station_load_1m_local', station_id);

-- == Загрузка линий по минутам ==
CREATE TABLE IF NOT EXISTS metro.line_load_1m_local ON CLUSTER metro_cluster (
    minute DateTime,
    line_id UInt16,
    direction Enum8('in' = 1, 'out' = 2),
    passengers SimpleAggregateFunction(sum, UInt64)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/line_load_1m', '{replica}')
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute, line_id, direction)
SETTINGS replicated_deduplication_window = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS metro.mv_line_load_1m ON CLUSTER metro_cluster
TO metro.line_load_1m_local AS
SELECT
    toStartOfMinute(event_time) AS minute,
    line_id,
    direction,
    count() AS passengers
FROM metro.turnstile_events
GROUP BY minute, line_id, direction;

CREATE TABLE IF NOT EXISTS metro.line_load_1m ON CLUSTER metro_cluster
AS metro.line_load_1m_local
ENGINE = Distributed('metro_cluster', 'metro', 'line_load_1m_local', line_id);

-- == Поезда: дельта посадок/высадок по минутам ==
-- onboard(t) = running sum delta по minute для train_id;
-- load factor = onboard / (num_cars * total_per_car).
CREATE TABLE IF NOT EXISTS metro.train_load_1m_local ON CLUSTER metro_cluster (
    train_id UInt32,
    minute DateTime,
    delta SimpleAggregateFunction(sum, Int64)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/train_load_1m', '{replica}')
PARTITION BY toYYYYMMDD(minute)
ORDER BY (train_id, minute)
SETTINGS replicated_deduplication_window = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS metro.mv_train_load_1m ON CLUSTER metro_cluster
TO metro.train_load_1m_local AS
SELECT
    train_id,
    toStartOfMinute(event_time) AS minute,
    sum(if(direction = 'in', toInt64(1), toInt64(-1))) AS delta
FROM metro.turnstile_events
WHERE train_id != 0
GROUP BY train_id, minute;

CREATE TABLE IF NOT EXISTS metro.train_load_1m ON CLUSTER metro_cluster
AS metro.train_load_1m_local
ENGINE = Distributed('metro_cluster', 'metro', 'train_load_1m_local', train_id);
