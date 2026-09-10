package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.connector.doris.DorisJdbcSupport
import org.apache.spark.internal.Logging

/**
 * Doris Partition Overwrite Manager
 *
 * Implements temporary-partition replacement consistent with Doris INSERT OVERWRITE.
 *   1. prepareTempPartitions  — query of main partitions Range create the same-range temporary partitions
 *   2. (External) Write to temp partition
 *   3. replacePartitions — REPLACE PARTITION Atomic Switch
 *   4. cleanupTempPartitions — Clean up remaining temp partitions on failure
 *
 * temp partition Naming convention: tmp_<formal_partition_name>_<jobTimestamp>tmp_<formal partition name>_<jobTimestamp>
 * Ensure there is no conflict during concurrent writes to the same partition, and timestamps can be used for tracing.
 *
 */
class DorisPartitionManager(config: DorisBatchConfig, jobTimestamp: Long)
  extends DorisJdbcSupport with Logging {

  /** Formal partition name → temp partition name mapping */
  val tempPartitionNames: Map[String, String] =
    config.targetPartitions.map(p => p -> s"tmp_${p}_$jobTimestamp").toMap

  /**
   * Create corresponding temp partition for each formal shard.
   * Query the source range with SHOW PARTITIONS and create a matching temporary partition.
   *
   * @throws RuntimeException Partition creation fails, RuntimeException is thrown. The caller should catch this exception and perform cleanup.
   */
  def prepareTempPartitions(): Unit = {
    logInfo(s"[DorisPartitionManager] Preparing ${tempPartitionNames.size} temp partition(s) " +
      s"for ${config.database}.${config.table}")

    // clear old temporary partitions from the same batch to avoid conflicts. Range conflict
    tempPartitionNames.values.foreach { temp =>
      val dropSql =
        s"ALTER TABLE `${config.database}`.`${config.table}` " +
        s"DROP TEMPORARY PARTITION IF EXISTS `$temp`"
      executeDorisSql(dropSql, config)
    }

    // a single query retrieves all active partitions ranges Range
    val partitionRanges = queryPartitionRanges()

    tempPartitionNames.foreach { case (formal, temp) =>
      val rangeValues = partitionRanges.getOrElse(formal,
        throw PistaErrors.dorisWriterError(
          s"[DorisPartitionManager] Formal partition '$formal' not found in ${config.database}.${config.table}"))

      val sql =
        s"ALTER TABLE `${config.database}`.`${config.table}` " +
        s"ADD TEMPORARY PARTITION IF NOT EXISTS `$temp` VALUES $rangeValues"

      val ok = executeDorisSql(sql, config)
      if (!ok)
        throw PistaErrors.dorisWriterError(
          s"[DorisPartitionManager] Failed to create temp partition '$temp' " +
          s"(formal='$formal', range=$rangeValues) for ${config.database}.${config.table}")

      logInfo(s"[DorisPartitionManager] Created temp partition: $temp (formal=$formal, range=$rangeValues)")
    }
  }

  /**
   * Atomic replacement: replace all temp partitions with their corresponding formal partitions.
   * Execute ALTER TABLE ... REPLACE PARTITION. Doris ensures atomicity.
   *
   * @throws RuntimeException Replacement fails and throws an exception when formal partition data is not damaged.
   */
  def replacePartitions(): Unit = {
    val formalList = tempPartitionNames.keys.map(p => s"`$p`").mkString(", ")
    val tempList   = tempPartitionNames.values.map(p => s"`$p`").mkString(", ")

    val sql =
      s"ALTER TABLE `${config.database}`.`${config.table}` " +
      s"REPLACE PARTITION ($formalList) WITH TEMPORARY PARTITION ($tempList) " +
      s"PROPERTIES ('strict_range' = 'false', 'use_temp_partition_name' = 'false')"

    logInfo(s"[DorisPartitionManager] Replacing partitions: $formalList → $tempList")

    val ok = executeDorisSql(sql, config)
    if (!ok)
      throw PistaErrors.dorisWriterError(
        s"[DorisPartitionManager] REPLACE PARTITION failed for ${config.database}.${config.table}. " +
        s"Formal partitions are intact; temp partitions need manual cleanup: ${tempList}")

    logInfo(s"[DorisPartitionManager] REPLACE PARTITION succeeded for ${config.database}.${config.table}")
  }

  /**
   * Clean temp partition (failing path reuse).
   * DROP TEMPORARY PARTITION IF EXISTS SPARINGLY IDENTITY SAFE.
   */
  def cleanupTempPartitions(): Unit = {
    logWarning(s"[DorisPartitionManager] Cleaning up ${tempPartitionNames.size} temp partition(s)")

    tempPartitionNames.values.foreach { temp =>
      val sql =
        s"ALTER TABLE `${config.database}`.`${config.table}` " +
        s"DROP TEMPORARY PARTITION IF EXISTS `$temp`"

      val ok = executeDorisSql(sql, config)
      if (ok)
        logInfo(s"[DorisPartitionManager] Dropped temp partition: $temp")
      else
        logWarning(s"[DorisPartitionManager] Failed to drop temp partition: $temp (may need manual cleanup)")
    }
  }

  // ========== Internal Implementation ==========

  /**
   * query information for all active partitions. Range information.
   *
   * SHOW PARTITIONS Returns the Range column format:
   *   [types: [BIGINT]; keys: [-9223372036854775808]; ..types: [BIGINT]; keys: [3000000]; )
   *
   * Parse into VALUES clause format:
   *   [("-9223372036854775808"), ("3000000"))
   */
  private def queryPartitionRanges(): Map[String, String] = {
    val sql = s"SHOW PARTITIONS FROM `${config.database}`.`${config.table}`"

    try {
      val conn = buildDorisJdbcConnection(config, Some(config.database))
      try {
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(sql)
        val ranges = Iterator.continually(rs.next()).takeWhile(identity).map { _ =>
          val partName = rs.getString("PartitionName")
          val rangeStr = rs.getString("Range")
          partName -> parseRangeToValues(rangeStr)
        }.toMap
        rs.close()
        stmt.close()
        ranges
      } finally conn.close()
    } catch {
      case e: Exception =>
        throw PistaErrors.dorisWriterError(
          s"[DorisPartitionManager] Failed to query partitions for ${config.database}.${config.table}: ${e.getMessage}", e)
    }
  }

  /**
   * Parse the Range column of SHOW PARTITIONS as a VALUES clause format.
   *
   * Range Format:
   *   [types: [TYPE_LOWER]; keys: [LOWER_VAL]; ..types: [TYPE_UPPER]; keys: [UPPER_VAL]; BOUNDARY
   *
   * convert to:
   *   [("LOWER_VAL"), ("UPPER_VAL")BOUNDARY
   *
   * where BOUNDARY for )(exclusiveor ](inclusive)
   */
  private[connector] def parseRangeToValues(rangeStr: String): String = {
    // A VALUE inside SHOW PARTITIONS keys: [...] may contain ], so bracket depth is unsafe.
    // The opening bracket is a format delimiter and is not part of the range value.
    // The final "]" before the next "[" is the closing bracket.
    def extractKeys(startIdx: Int): (String, Int) = {
      val keysIdx = rangeStr.indexOf("keys:", startIdx)
      if (keysIdx < 0)
        throw PistaErrors.dorisWriterError(s"[DorisPartitionManager] Cannot find 'keys:' in: $rangeStr")
      val bracketStart = rangeStr.indexOf('[', keysIdx)
      if (bracketStart < 0)
        throw PistaErrors.dorisWriterError(s"[DorisPartitionManager] Cannot find '[' after 'keys:' in: $rangeStr")

      val nextBracket = rangeStr.indexOf('[', bracketStart + 1)
      val searchEnd = if (nextBracket >= 0) nextBracket else rangeStr.length - 1 // Exclude the trailing boundary character

      var closeIdx = -1
      var pos = bracketStart + 1
      while (pos < searchEnd) {
        if (rangeStr.charAt(pos) == ']') closeIdx = pos
        pos += 1
      }
      if (closeIdx < 0)
        throw PistaErrors.dorisWriterError(s"[DorisPartitionManager] Cannot find ']' after keys value in: $rangeStr")

      val content = rangeStr.substring(bracketStart + 1, closeIdx)
      (content, closeIdx + 1) // Skip "]"
    }

    val (lower, afterLowerIdx) = extractKeys(0)
    val (upper, _) = extractKeys(afterLowerIdx)

    val boundary = rangeStr.last match {
      case ')' | ']' => rangeStr.last.toString
      case _ =>
        throw PistaErrors.dorisWriterError(s"[DorisPartitionManager] Unknown boundary in: $rangeStr")
    }

    s"[${formatValue(lower)}, ${formatValue(upper)}$boundary"
  }

  /** Format partition key values as literal values in the VALUES clause. Do not quote MAXVALUE/MINVALUE, quote others. */
  private def formatValue(v: String): String =
    if (v == "MAXVALUE" || v == "MINVALUE") s"($v)" else s"""("$v")"""
}
