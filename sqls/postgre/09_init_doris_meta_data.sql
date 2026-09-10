-- Doris metadata initialization data
-- For testing and development environments (corresponding to the local Docker Compose doris-cluster)

-- Initialize example FE node information (single FE, using Docker service name)
INSERT INTO doris_fe_info (cluster_name, fe_host, fe_query_port, fe_http_port, is_leader)
VALUES
    ('example_doris_cluster', 'doris-fe', 9030, 8030, true)
ON CONFLICT (cluster_name, fe_host) DO NOTHING;
