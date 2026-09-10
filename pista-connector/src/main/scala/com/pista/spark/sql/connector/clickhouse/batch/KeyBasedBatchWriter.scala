package com.pista.spark.sql.connector.clickhouse.batch

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{call_udf, col}

/**
 * Column hash-based batch write for ClickHouse Spark task
 *
 * Partition DataFrame rows by primary key hashCode modulo across different ClickHouse shards.
 * Use private compatible hash function of connector. Preserve existing shard mapping.
 *
 * This strategy is suitable for ClickHouse clusters that are sharded by primary key, where data with the same primary key is always routed to the same node.
 * Preserves key locality across shards to support ReplicatedMergeTree deduplication.
 *
 * Uses the connector-private ck_legacy_abs_hashcode function registered by ClickHouseBatchWriter.
 *
 * Migrated KeyBasedBatchWriter from the legacy implementation, main changes:
 * - CONFIG THROUGH ClickHouseBatchConfig
 * - filter logic is extracted to initDataFrame, decoupling it from the write process. initDataFrameno longer coupled with the write process
 *
 * @param config          Batch write configuration
 * @param index           Current shard index
 * @param machineCount    Total shards
 * @param sourceDataFrame Loaded source DataFrame (initialized by caller)
 * @param primaryKey      optional primary-key column; hashes into machineCount shards, or uses the full DataFrame when absent
 * @param verbose         Whether to print detailed logs (specified by ClickHouseBatchSubmitter based on pending.min)
 */
class KeyBasedBatchWriter(
  config: ClickHouseBatchConfig,
  index: Int,
  machineCount: Int,
  sourceDataFrame: DataFrame,
  primaryKey: Option[String],
  verbose: Boolean = true
) extends ClickHouseBatchWriter(config, index, machineCount, verbose) {

  override protected def loadShardData: DataFrame = primaryKey match {
    case Some(pk) =>
      // FIXME(PISTA-ROUTING): Replace this compatibility route with the
      // versioned RoutingContract (native xxhash64/pmod, fingerprint, and
      // fail-closed invalid-key handling). Keep this bridge until that
      // migration is landed and verified with routing golden vectors and
      // ClickHouse recovery IT.
      sourceDataFrame.where(
        col(pk).isNotNull
          .and(col(pk).cast("string") =!= "")
          .and(call_udf("ck_legacy_abs_hashcode", col(pk).cast("string")) % machineCount === index))
    case None =>
      sourceDataFrame
  }
}

object KeyBasedBatchWriter {

  /**
   * Factory Method: Create an instance of KeyBasedBatchWriter
   *
   * @param config          Batch write configuration
   * @param index           Current shard index
   * @param machineCount    Total number of shards
   * @param sourceDf        Source DataFrame (owned by the caller for caching)
   * @param primaryKey      Primary column name (optional) for hash partition routing compatibility
   * @param verbose         Whether to print detailed logs (default true)
   */
  def apply(
    config: ClickHouseBatchConfig,
    index: Int,
    machineCount: Int,
    sourceDf: DataFrame,
    primaryKey: Option[String],
    verbose: Boolean = true
  ): KeyBasedBatchWriter =
    new KeyBasedBatchWriter(config, index, machineCount, sourceDf, primaryKey, verbose)
}
