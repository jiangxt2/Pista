-- Doris Task Information Table DDL
-- Track task-level metadata for three write modes (spark_connector/stream_load/broker_load)
DROP TABLE IF EXISTS doris_task_info CASCADE;

CREATE TABLE doris_task_info (
    id SERIAL NOT NULL,
    task_id VARCHAR(256) NOT NULL,
    cluster_name VARCHAR(128) NOT NULL,
    dbname VARCHAR(128) NOT NULL,
    tbname VARCHAR(128) NOT NULL,
    rdate TEXT NOT NULL,
    write_mode VARCHAR(32) NOT NULL,
    source_table VARCHAR(256),
    partition_filter TEXT,
    expected_rows BIGINT NOT NULL DEFAULT -1,
    written_rows BIGINT NOT NULL DEFAULT -1,
    status INTEGER NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    -- Broker Load fields; null for other write modes.
    load_job_id VARCHAR(256),
    broker_path VARCHAR(1024),
    delete_hdfs_after_load BOOLEAN NOT NULL DEFAULT false,
    mtimestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (task_id)
);

COMMENT ON TABLE doris_task_info IS 'Doris task records for idempotency and state tracking across supported write modes';
COMMENT ON COLUMN doris_task_info.id IS 'primary key';
COMMENT ON COLUMN doris_task_info.task_id IS 'Unique task identifier: cluster_labelPrefix_database_table_partition';
COMMENT ON COLUMN doris_task_info.cluster_name IS 'Cluster name referencing doris_fe_info.cluster_name';
COMMENT ON COLUMN doris_task_info.dbname IS 'Target database name';
COMMENT ON COLUMN doris_task_info.tbname IS 'Target table name';
COMMENT ON COLUMN doris_task_info.rdate IS 'Partition date or normalized partition filter';
COMMENT ON COLUMN doris_task_info.write_mode IS 'Write mode: spark_connector, stream_load, or broker_load';
COMMENT ON COLUMN doris_task_info.source_table IS 'Optional source table in catalog notation';
COMMENT ON COLUMN doris_task_info.partition_filter IS 'Optional source partition predicate';
COMMENT ON COLUMN doris_task_info.expected_rows IS 'Expected row count';
COMMENT ON COLUMN doris_task_info.written_rows IS 'Written row count after successful completion';
COMMENT ON COLUMN doris_task_info.status IS 'Task state: 0=RUNNING, 1=SUCCESS, 2=FAILURE, 3=CANCELLED';
COMMENT ON COLUMN doris_task_info.start_time IS 'Task start time';
COMMENT ON COLUMN doris_task_info.end_time IS 'Task end time';
COMMENT ON COLUMN doris_task_info.load_job_id IS 'Broker Load job identifier';
COMMENT ON COLUMN doris_task_info.broker_path IS 'Broker Load source path';
COMMENT ON COLUMN doris_task_info.delete_hdfs_after_load IS 'Whether Broker Load source files are deleted after loading';
COMMENT ON COLUMN doris_task_info.mtimestamp IS 'Last update time';

-- Keep mtimestamp current on updates.
CREATE TRIGGER update_doris_task_info_mtimestamp
BEFORE UPDATE ON doris_task_info
FOR EACH ROW EXECUTE FUNCTION update_mtimestamp();

-- Query indexes.
CREATE INDEX idx_doris_task_info_cluster ON doris_task_info(cluster_name);
CREATE INDEX idx_doris_task_info_status ON doris_task_info(status);
CREATE INDEX idx_doris_task_info_start_time ON doris_task_info(start_time DESC);
