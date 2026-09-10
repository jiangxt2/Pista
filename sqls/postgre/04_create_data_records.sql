-- Data Records Table DDL
DROP TABLE IF EXISTS clickhouse_data_records;

CREATE TABLE clickhouse_data_records (
    id SERIAL NOT NULL,
    dbname VARCHAR(128) NOT NULL,
    tbname VARCHAR(128) NOT NULL,
    rdate VARCHAR(128) NOT NULL,
    cluster_id INTEGER NOT NULL,
    data_index INTEGER NOT NULL,
    index_size INTEGER NOT NULL,
    original_data_volume BIGINT NOT NULL,
    insert_data_volume BIGINT NOT NULL,
    host_address VARCHAR(128) NOT NULL,
    status INTEGER NOT NULL,
    mtimestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

COMMENT ON COLUMN clickhouse_data_records.id IS 'primary key';
COMMENT ON COLUMN clickhouse_data_records.dbname IS 'database name';
COMMENT ON COLUMN clickhouse_data_records.tbname IS 'table_name';
COMMENT ON COLUMN clickhouse_data_records.rdate IS 'date partition';
COMMENT ON COLUMN clickhouse_data_records.cluster_id IS 'cluster_id represents the storage cluster';
COMMENT ON COLUMN clickhouse_data_records.data_index IS 'shard';
COMMENT ON COLUMN clickhouse_data_records.index_size IS 'total data shards';
COMMENT ON COLUMN clickhouse_data_records.original_data_volume IS 'original data volume';
COMMENT ON COLUMN clickhouse_data_records.insert_data_volume IS 'write data volume';
COMMENT ON COLUMN clickhouse_data_records.host_address IS 'machine corresponding to data slice';
COMMENT ON COLUMN clickhouse_data_records.status IS 'Task state: 0=INITIAL, 1=SUCCESS, 2=RUNNING, -1=FAILURE';
COMMENT ON COLUMN clickhouse_data_records.mtimestamp IS 'update time';

CREATE TRIGGER update_clickhouse_data_records_mtimestamp
BEFORE UPDATE ON clickhouse_data_records
FOR EACH ROW EXECUTE FUNCTION update_mtimestamp();
