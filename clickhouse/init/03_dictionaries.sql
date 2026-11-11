-- Словари поверх справочников: на вставке тащить line_id и имена для панелей
-- без join. ON CLUSTER — чтобы словарь был на каждом узле кластера.
CREATE DICTIONARY IF NOT EXISTS metro.dict_lines ON CLUSTER metro_cluster (
    line_id UInt16,
    name String,
    color String
) PRIMARY KEY line_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'metro' TABLE 'lines'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED());

CREATE DICTIONARY IF NOT EXISTS metro.dict_stations ON CLUSTER metro_cluster (
    station_id UInt16,
    line_id UInt16,
    name String,
    lat Float64,
    lon Float64
) PRIMARY KEY station_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'metro' TABLE 'stations'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED());

CREATE DICTIONARY IF NOT EXISTS metro.dict_trains ON CLUSTER metro_cluster (
    train_id UInt32,
    line_id UInt16,
    num_cars UInt8,
    seated_per_car UInt16,
    total_per_car UInt16
) PRIMARY KEY train_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' DB 'metro' TABLE 'trains'))
LIFETIME(MIN 60 MAX 120)
LAYOUT(HASHED());
