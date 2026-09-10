package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.connector.doris.DorisJdbcSupport
import org.apache.spark.internal.Logging

/**
 * Doris full table overwrite manager
 *
 * Implement an atomic replacement process for a temporary table with ALTER TABLE REPLACE, symmetric to DorisPartitionManager:
 *   1. createTempTable    — use CREATE TABLE LIKE to create a temporary table with the same schema
 *   2. (External) Write to a Temporary Table
 *   3. replaceTable       — ALTER TABLE REPLACE WITH TABLE Atomic Replacement Conversion
 *   4. cleanupTempTable — Clean up residual temporary tables on failure
 *
 * Temporary table naming convention: tmp_<target_table>_<jobTimestamp>
 * Ensure there is no conflict during concurrent writes to the same table, and conflicts can be traced using timestamps.
 *
 * @param config       Batch write configuration (including target database table information)
 * @param jobTimestamp Job Timestamp (used for temporary table naming)
 */
class DorisTableSwapManager(config: DorisBatchConfig, jobTimestamp: Long)
  extends DorisJdbcSupport with Logging {

  val tempTableName: String = s"tmp_${config.table}_$jobTimestamp"

  /**
   * Create a temporary table with the same schema as the original table (including buckets, replicas, and attributes).
   *
   * @throws PistaErrors.dorisWriterError Throws an exception when creation fails.
   */
  def createTempTable(): Unit = {
    val sql =
      s"CREATE TABLE IF NOT EXISTS `${config.database}`.`$tempTableName` " +
        s"LIKE `${config.database}`.`${config.table}`"

    logInfo(s"[DorisTableSwapManager] Creating temp table: $tempTableName")

    val ok = executeDorisSql(sql, config)
    if (!ok)
      throw PistaErrors.dorisWriterError(
        s"[DorisTableSwapManager] Failed to create temp table $tempTableName " +
          s"for ${config.database}.${config.table}")

    logInfo(s"[DorisTableSwapManager] Created temp table: $tempTableName")
  }

  /**
   * Atomic replacement: REPLACE TABLE WITH TABLE PROPERTIES(swap->false)ALTER TABLE ... REPLACE WITH TABLE ... PROPERTIES("swap"="false")
   * Doris executes under db.writeLock + table.writeLock protection, writing to EditLog.
   *
   * @throws PistaErrors.dorisWriterError Replace fails and throws an exception, indicating that the old table data is not damaged.
   */
  def replaceTable(): Unit = {
    val sql =
      s"ALTER TABLE `${config.table}` " +
        s"REPLACE WITH TABLE `$tempTableName` " +
        s"""PROPERTIES("swap"="false")"""

    logInfo(s"[DorisTableSwapManager] Replacing table: ${config.table} → $tempTableName")

    val ok = executeDorisSql(sql, config)
    if (!ok)
      throw PistaErrors.dorisWriterError(
        s"[DorisTableSwapManager] ALTER TABLE REPLACE failed for ${config.database}.${config.table}. " +
          s"Temp table $tempTableName needs manual cleanup.")

    logInfo(s"[DorisTableSwapManager] ALTER TABLE REPLACE succeeded for ${config.database}.${config.table}")
  }

  /**
   * Clean temporary table (failing path invocation).
   * using DROP TABLE IF EXISTS for idempotent security. DROP TABLE IF EXISTSidempotent security.
   */
  def cleanupTempTable(): Unit = {
    logWarning(s"[DorisTableSwapManager] Cleaning up temp table: $tempTableName")

    val sql = s"DROP TABLE IF EXISTS `${config.database}`.`$tempTableName`"
    val ok = executeDorisSql(sql, config)

    if (ok)
      logInfo(s"[DorisTableSwapManager] Dropped temp table: $tempTableName")
    else
      logWarning(s"[DorisTableSwapManager] Failed to drop temp table: $tempTableName (may need manual cleanup)")
  }
}
