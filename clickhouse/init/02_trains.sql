-- Парк поездов: плоская модель — число вагонов и вместимость одного вагона
-- (сидячих/всего). Обновляется генератором; Replacing по updated_at.
-- Как и справочники — полный набор на каждом узле (путь ZK без {shard}).
CREATE TABLE IF NOT EXISTS metro.trains ON CLUSTER metro_cluster (
    train_id UInt32,
    line_id UInt16,
    num_cars UInt8,
    seated_per_car UInt16,
    total_per_car UInt16,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/dims/trains', '{replica}', updated_at)
ORDER BY train_id;
