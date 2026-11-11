-- Полный сброс схемы, созданной файлами 01-06.
-- Зачем: /docker-entrypoint-initdb.d прогоняется только на пустом томе. Если схему
-- нужно пересоздать на живом кластере (поправил DDL, ушла валидация), сначала
-- этот файл, потом 01-06 целиком — см. make reinit.
-- Порядок обратный зависимостям: MV приёмника и сама kafka-таблица, затем
-- агрегационные MV и их цели, факт, словари, справочники. Всё IF EXISTS —
-- при первой инициализации на пустом кластере файл просто ничего не трогает.
-- SYNC обязателен: узел реплики в ZK после обычного DROP чистится фоном, и
-- следом идущий CREATE может получить REPLICA_ALREADY_EXISTS.

DROP TABLE IF EXISTS metro.mv_kafka_events ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.kafka_events ON CLUSTER metro_cluster SYNC;

DROP TABLE IF EXISTS metro.mv_station_load_1m ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.mv_line_load_1m ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.mv_train_load_1m ON CLUSTER metro_cluster SYNC;

DROP TABLE IF EXISTS metro.station_load_1m ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.station_load_1m_local ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.line_load_1m ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.line_load_1m_local ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.train_load_1m ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.train_load_1m_local ON CLUSTER metro_cluster SYNC;

DROP TABLE IF EXISTS metro.turnstile_events_all ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.turnstile_events ON CLUSTER metro_cluster SYNC;

DROP DICTIONARY IF EXISTS metro.dict_trains ON CLUSTER metro_cluster;
DROP DICTIONARY IF EXISTS metro.dict_stations ON CLUSTER metro_cluster;
DROP DICTIONARY IF EXISTS metro.dict_lines ON CLUSTER metro_cluster;

DROP TABLE IF EXISTS metro.trains ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.stations ON CLUSTER metro_cluster SYNC;
DROP TABLE IF EXISTS metro.lines ON CLUSTER metro_cluster SYNC;
