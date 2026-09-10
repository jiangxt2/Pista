package com.pista.spark.sql.clickhouse.meta.record

/** Metadata Record Base Class */
sealed trait Record

/**
 * shard write task records*
 * Each index (shard) corresponds to one record, tracking write status, machine assignment, and data volume.
 */
case class DataRecord(
  id: Int = 0,
  dbName: String,
  tbName: String,
  rDate: String,
  clusterId: Int,
  dataIndex: Int,
  indexSize: Int,
  originalDataVolume: Long = 0L,
  insertDataVolume: Long = 0L,
  hostAddress: String = "",
  status: Int = RecordStatus.INITIAL_VALUE
) extends Record

/**
 * machine information record
 * Read cluster node information from clickhouse_machine_info table
 */
case class MachineRecord(
  id: Int = 0,
  clusterId: Int,
  shardNum: Int = 0,
  replicaNum: Int = 1,
  hostAddress: String = "",
  onlyRole: Int = 0,
  isAlive: Int = 1
) extends Record

/**
 * cluster records
 * Read cluster ID from clickhouse_cluster_info table
 */
case class ClusterRecord(id: Int = 0, clusterName: String) extends Record

/**
 * Global task information logging
 * Aggregate the overall state and data volume across all shards.
 */
case class DataInfoRecord(
  id: Int = 0,
  dbName: String,
  tbName: String,
  rDate: String,
  clusterId: Int,
  indexSize: Int,
  dataVolume: Long = 0L,
  status: Int = RecordStatus.INITIAL_VALUE
) extends Record
