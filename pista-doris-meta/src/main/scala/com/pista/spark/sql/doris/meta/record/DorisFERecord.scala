package com.pista.spark.sql.doris.meta.record

import java.sql.Timestamp

/**
 *doris_fe_info table domain object*
 *
 * @param id          primary_key
 * @param clusterName association doris_cluster_info.cluster_name
 * @param feHost      FE hostname or IP
 * @param feQueryPort MySQL Protocol Port (default 9030)
 * @param feHttpPort HTTP Port (default 8030)
 * @param isLeader    isLeader Master FE
 * @param timestamp update\_row
 */
case class DorisFERecord(
  id:          Long,
  clusterName: String,
  feHost:      String,
  feQueryPort: Int,
  feHttpPort:  Int,
  isLeader:    Boolean,
  mtimestamp:  Timestamp
)
