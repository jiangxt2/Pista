-- Doris FE Node Information Table DDL
-- Simplify spark-submit parameters (optional)
DROP TABLE IF EXISTS doris_fe_info;

CREATE TABLE doris_fe_info (
    id SERIAL NOT NULL,
    cluster_name VARCHAR(128) NOT NULL,
    fe_host VARCHAR(128) NOT NULL,
    fe_query_port INTEGER NOT NULL DEFAULT 9030,
    fe_http_port INTEGER NOT NULL DEFAULT 8030,
    is_leader BOOLEAN NOT NULL DEFAULT false,
    mtimestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE (cluster_name, fe_host)
);

COMMENT ON TABLE doris_fe_info IS 'Doris FE node information used to resolve submitter endpoints';
COMMENT ON COLUMN doris_fe_info.id IS 'primary key';
COMMENT ON COLUMN doris_fe_info.cluster_name IS 'cluster name (corresponding to doris_cluster_info.cluster_name)';
COMMENT ON COLUMN doris_fe_info.fe_host IS 'FE hostname or IP address';
COMMENT ON COLUMN doris_fe_info.fe_query_port IS 'MySQL protocol port (default 9030)';
COMMENT ON COLUMN doris_fe_info.fe_http_port IS 'HTTP protocol port (default 8030)';
COMMENT ON COLUMN doris_fe_info.is_leader IS 'Whether this node is the leader FE';
COMMENT ON COLUMN doris_fe_info.mtimestamp IS 'update time';

-- auto-update mtimestamp trigger persisted
CREATE TRIGGER update_doris_fe_info_mtimestamp
BEFORE UPDATE ON doris_fe_info
FOR EACH ROW EXECUTE FUNCTION update_mtimestamp();
