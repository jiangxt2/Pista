-- Data Information Table DDL
DROP TABLE IF EXISTS clickhouse_data_info;

CREATE TABLE clickhouse_data_info (
    id SERIAL NOT NULL,
    dbname VARCHAR(128) NOT NULL,
    tbname VARCHAR(128) NOT NULL,
    rdate VARCHAR(128) NOT NULL,
    cluster_id INTEGER NOT NULL,
    index_size INTEGER NOT NULL,
    data_volume BIGINT NOT NULL,
    status INTEGER NOT NULL,
    mtimestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

COMMENT ON COLUMN clickhouse_data_info.id IS 'primary key';
COMMENT ON COLUMN clickhouse_data_info.dbname IS 'database name';
COMMENT ON COLUMN clickhouse_data_info.tbname IS 'table_name';
COMMENT ON COLUMN clickhouse_data_info.rdate IS 'date partition';
COMMENT ON COLUMN clickhouse_data_info.cluster_id IS 'cluster_id represents the storage cluster';
COMMENT ON COLUMN clickhouse_data_info.index_size IS 'number of partitions';
COMMENT ON COLUMN clickhouse_data_info.data_volume IS 'Data volume written';
COMMENT ON COLUMN clickhouse_data_info.status IS 'Task state: 0=INITIAL, 1=SUCCESS, 2=RUNNING, -1=FAILURE';
COMMENT ON COLUMN clickhouse_data_info.mtimestamp IS 'update time';

CREATE TRIGGER update_clickhouse_data_info_mtimestamp
BEFORE UPDATE ON clickhouse_data_info
FOR EACH ROW EXECUTE FUNCTION update_mtimestamp();
