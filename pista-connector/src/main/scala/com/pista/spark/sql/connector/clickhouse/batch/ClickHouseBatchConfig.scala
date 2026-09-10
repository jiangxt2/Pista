package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.connector.BatchWriteConfig

/**
 * ClickHouse batch write configuration
 *
 * Replaces the legacy global ExecuteOptions object with an immutable case class.
 * Multi-instance concurrency (each shard task configured independently), eliminating global state race conditions for objects.
 *
 * @param database         Target database name
 * @param table            Target table name
 * @param jdbcUrl          ClickHouse JDBC URL
 * @param options          JDBC connection options (user/password/socket_timeout, etc.)
 * @param batchSize        JDBC Batch Write Size
 * @param outputPartitions Coalesce target partitions before write.
 * @param overwrite        Overwrite existing write (reset metadata state)
 * @param convertNullToDefault Whether to convert null to default values for each type
 * @param partitionDate Partition date value (e.g., "20260402"), overwrite=true when used for DROP PARTITION;
 *                         Empty string executes TRUNCATE TABLE
 */
case class ClickHouseBatchConfig(
  database: String,
  table: String,
  jdbcUrl: String,
  options: Map[String, String] = Map.empty,
  batchSize: Long = 200000L,
  outputPartitions: Int = 10,
  overwrite: Boolean = false,
  overwriteMode: String = "on_cluster", // overwrite mode: on_cluster, // overwrite mode: overwrite or per-host overwrite modeon_cluster or per_host
  convertNullToDefault: Boolean = false,
  partitionDate: String = "",
  partitionColumn: String = "",     // Date partition column, used with partitionDate for post-write COUNT validation
  skipLocalDelete: Boolean = false, // Skip local deletion (true when global deletion has been executed)
  clearDataMaxAttempts: Int = 30,    // Maximum number of validation checks for data clearance
  clearDataIntervalMs: Long = 10000L, // Clear data validation check interval (milliseconds)
  maxBackupSwitches: Int = 1          // Maximum switch count for backup instances (0 = disabled)
) extends BatchWriteConfig
