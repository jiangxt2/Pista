-- Cluster Information Table DDL
DROP TABLE IF EXISTS clickhouse_cluster_info;

CREATE TABLE clickhouse_cluster_info (
    id SERIAL NOT NULL,
    cluster_name VARCHAR(128) NOT NULL,
    PRIMARY KEY (id)
);

COMMENT ON COLUMN clickhouse_cluster_info.id IS 'primary key';
COMMENT ON COLUMN clickhouse_cluster_info.cluster_name IS 'Unique cluster name';

-- Initial Data
-- ClickHouse Cluster Name:ck_cluster
INSERT INTO clickhouse_cluster_info (cluster_name) VALUES ('ck_cluster');
