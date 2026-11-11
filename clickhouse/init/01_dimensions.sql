-- Справочники: линии и станции. Таблицы маленькие, поэтому лежат целиком на каждом
-- узле — один набор реплик на весь кластер (путь в ZK без {shard}).
-- Сид грузится из ./data, он примонтирован в user_files/metro_seed.

CREATE TABLE IF NOT EXISTS metro.lines ON CLUSTER metro_cluster (
    line_id UInt16,
    name LowCardinality(String),
    color LowCardinality(String)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/dims/lines', '{replica}')
ORDER BY line_id;

CREATE TABLE IF NOT EXISTS metro.stations ON CLUSTER metro_cluster (
    station_id UInt16,
    line_id UInt16,
    name String,
    lat Float64,
    lon Float64,
    seq UInt16
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/dims/stations', '{replica}')
ORDER BY station_id;

INSERT INTO metro.lines
SELECT line_id, name, color
FROM file('metro_seed/lines.csv', 'CSVWithNames',
          'line_id UInt16, name String, color String');

INSERT INTO metro.stations
SELECT station_id, line_id, name, lat, lon, seq
FROM file('metro_seed/stations.csv', 'CSVWithNames',
          'station_id UInt16, line_id UInt16, name String, lat Float64, lon Float64, seq UInt16');
