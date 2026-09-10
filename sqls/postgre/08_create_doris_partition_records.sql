-- Doris Partition Records Table DDL
-- Partition-level Stream Load state for retry and recovery.
DROP TABLE IF EXISTS doris_partition_records;

CREATE TABLE doris_partition_records (
    id SERIAL NOT NULL,
    task_id VARCHAR(256) NOT NULL,
    partition_id INTEGER NOT NULL,
    attempt_id INTEGER NOT NULL DEFAULT 0,
    label VARCHAR(512) NOT NULL,
    written_rows BIGINT NOT NULL DEFAULT 0,
    status INTEGER NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    mtimestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (task_id, partition_id, attempt_id)
);

COMMENT ON TABLE doris_partition_records IS 'Doris Stream Load partition records for retry and recovery';
COMMENT ON COLUMN doris_partition_records.id IS 'primary key';
COMMENT ON COLUMN doris_partition_records.task_id IS 'Task identifier referencing doris_task_info';
COMMENT ON COLUMN doris_partition_records.partition_id IS 'Spark Partition ID';
COMMENT ON COLUMN doris_partition_records.attempt_id IS 'Attempt number: 0 for the initial attempt, 1 for the first retry';
COMMENT ON COLUMN doris_partition_records.label IS 'Idempotent Stream Load label in prefix_pN_aM format';
COMMENT ON COLUMN doris_partition_records.written_rows IS 'Actual written row count';
COMMENT ON COLUMN doris_partition_records.status IS 'Partition state: 0=RUNNING, 1=SUCCESS, 2=FAILURE';
COMMENT ON COLUMN doris_partition_records.start_time IS 'Partition attempt start time';
COMMENT ON COLUMN doris_partition_records.end_time IS 'Partition attempt end time';
COMMENT ON COLUMN doris_partition_records.mtimestamp IS 'Last update time';

-- Keep mtimestamp current on updates.
CREATE TRIGGER update_doris_partition_records_mtimestamp
BEFORE UPDATE ON doris_partition_records
FOR EACH ROW EXECUTE FUNCTION update_mtimestamp();

-- Query indexes.
CREATE INDEX idx_doris_partition_records_task_id ON doris_partition_records(task_id);
CREATE INDEX idx_doris_partition_records_status ON doris_partition_records(status);

-- Foreign key constraint (referencing doris_task_info)
ALTER TABLE doris_partition_records
ADD CONSTRAINT fk_partition_task
FOREIGN KEY (task_id)
REFERENCES doris_task_info(task_id)
ON DELETE CASCADE;
