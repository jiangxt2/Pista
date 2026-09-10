-- Machine Information Table DDL
DROP TABLE IF EXISTS clickhouse_machine_info;

CREATE TABLE clickhouse_machine_info (
    id SERIAL NOT NULL,
    cluster_id INTEGER NOT NULL,
    shard_num INTEGER NOT NULL,
    replica_num INTEGER NOT NULL,
    host_address VARCHAR(128) NOT NULL,
    only_role INTEGER NOT NULL,
    is_alive INTEGER NOT NULL,
    PRIMARY KEY (id)
);

COMMENT ON COLUMN clickhouse_machine_info.id IS 'primary key';
COMMENT ON COLUMN clickhouse_machine_info.cluster_id IS 'cluster_id';
COMMENT ON COLUMN clickhouse_machine_info.shard_num IS 'shard_num';
COMMENT ON COLUMN clickhouse_machine_info.replica_num IS 'replica_num';
COMMENT ON COLUMN clickhouse_machine_info.host_address IS 'host_address';
COMMENT ON COLUMN clickhouse_machine_info.only_role IS 'Is the role unique?';
COMMENT ON COLUMN clickhouse_machine_info.is_alive IS 'Can the machine write data?';
