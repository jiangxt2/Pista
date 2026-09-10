package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import org.apache.spark.internal.Logging

/**
 * Doris Task Metadata Manager
 *
 * Provide core capabilities such as task deduplication, state tracking, and observability:
 * - isSucceeded: Check if the task succeeded (to prevent duplicates)
 * - upsertRunning: Mark the task as an executing state
 * - markSuccess: mark the task as successful status
 * - markFailure: mark the task as failed status
 *
 * @param connection PostgreSQL connection
 * @param metaConf  meta data configuration
 *
 */
class DorisMetaManager(
  connection: DorisMetaConnection,
  metaConf: DorisMetaConf
) extends Logging {

  /**
   * Check if the task has succeeded
   *
   * @param taskId Task ID
   * @return true=The task succeeded.false=failure occurred or task was not executed
   */
  def isSucceeded(taskId: String): Boolean = {
    val sql =
      s"""
         |SELECT status
         |FROM doris_task_info
         |WHERE task_id = ? AND status = ${DorisRecordStatus.SUCCESS.id}
         |""".stripMargin

    try {
      val conn = connection.getConnection
      try {
        val ps = conn.prepareStatement(sql)
        ps.setString(1, taskId)
        val rs = ps.executeQuery()
        val result = rs.next()
        rs.close()
        ps.close()
        result
      } finally conn.close()
    } catch {
      case e: Exception =>
        logError(s"[DorisMetaManager] Task-status check failed with ${e.getClass.getSimpleName}")
        false
    }
  }

  /**
   * Check for any historical records of tasks (regardless of status)
   *
   * For 2PC validation, to distinguish between the "first run" and "expired transaction cleanup" scenarios.
   *
   * @param taskId Task ID
   * @return true=there are historical records, there exist historical records, there are records in the past data, there are historical records present.false=no records
   */
  def hasAnyRecord(taskId: String): Boolean = {
    val sql = "SELECT 1 FROM doris_task_info WHERE task_id = ?"
    try {
      val conn = connection.getConnection
      try {
        val ps = conn.prepareStatement(sql)
        ps.setString(1, taskId)
        val rs = ps.executeQuery()
        val result = rs.next()
        rs.close()
        ps.close()
        result
      } finally conn.close()
    } catch {
      case e: Exception =>
        logError(s"[DorisMetaManager] Task-history check failed with ${e.getClass.getSimpleName}")
        false
    }
  }

  /**
   * mark the task as running state
   *
   * Use UPSERT semantics: insert if the record does not exist, update the status to RUNNING if it exists.
   *
   * @param taskId Task ID
   * @param database Database name
   * @param table Table name
   * @param partitionDate Partition Date
   * @param writeMode Write Mode
   * @param sourceTable Source table name
   * @param partitionFilter Partition filter condition
   */
  def upsertRunning(
    taskId:          String,
    database:        String,
    table:           String,
    partitionDate:   String,
    writeMode:       String,
    sourceTable:     Option[String],
    partitionFilter: Option[String]
  ): Unit = {
    val sql =
      s"""
         |INSERT INTO doris_task_info (
         |  task_id, cluster_name, dbname, tbname, rdate,
         |  write_mode, source_table, partition_filter,
         |  expected_rows, written_rows, status,
         |  start_time
         |) VALUES (
         |  ?, ?, ?, ?, ?,
         |  ?, ?, ?,
         |  -1, -1, ${DorisRecordStatus.RUNNING.id},
         |  NOW()
         |)
         |ON CONFLICT (task_id) DO UPDATE SET
         |  status = ${DorisRecordStatus.RUNNING.id},
         |  start_time = NOW(),
         |  end_time = NULL
         |""".stripMargin

    try {
      val conn = connection.getConnection
      try {
        val ps = conn.prepareStatement(sql)
        ps.setString(1, taskId)
        ps.setString(2, metaConf.clusterName)
        ps.setString(3, database)
        ps.setString(4, table)
        ps.setString(5, partitionDate)
        ps.setString(6, writeMode)
        ps.setString(7, sourceTable.getOrElse(""))
        ps.setString(8, partitionFilter.orNull)
        ps.executeUpdate()
        ps.close()
        logInfo(s"[DorisMetaManager] Task $taskId marked as RUNNING")
      } finally conn.close()
    } catch {
      case e: Exception =>
        logError(s"[DorisMetaManager] RUNNING transition failed with ${e.getClass.getSimpleName}")
        throw e
    }
  }

  /**
   * mark task as successful state
   *
   * @param taskId Task ID
   * @param rowCount Number of rows written
   */
  def markSuccess(taskId: String, rowCount: Long): Unit = {
    val sql =
      s"""
         |UPDATE doris_task_info
         |SET
         |  written_rows = ?,
         |  status = ${DorisRecordStatus.SUCCESS.id},
         |  end_time = NOW()
         |WHERE task_id = ?
         |""".stripMargin

    try {
      val conn = connection.getConnection
      try {
        val ps = conn.prepareStatement(sql)
        ps.setLong(1, rowCount)
        ps.setString(2, taskId)
        val updated = ps.executeUpdate()
        ps.close()
        logInfo(s"[DorisMetaManager] Task $taskId marked as SUCCESS (rowCount=$rowCount, updated=$updated)")
      } finally conn.close()
    } catch {
      case e: Exception =>
        logError(s"[DorisMetaManager] SUCCESS transition failed with ${e.getClass.getSimpleName}")
        throw e
    }
  }

  /**
   * mark the task as failed state
   *
   * @param taskId       Task ID
   * @param errorMessage Error message
   */
  def markFailure(taskId: String, errorMessage: String): Unit = {
    val sql =
      s"""
         |UPDATE doris_task_info
         |SET
         |  status = ${DorisRecordStatus.FAILURE.id},
         |  end_time = NOW()
         |WHERE task_id = ?
         |""".stripMargin

    try {
      val conn = connection.getConnection
      try {
        val ps = conn.prepareStatement(sql)
        ps.setString(1, taskId)
        val updated = ps.executeUpdate()
        ps.close()
        logWarning(s"[DorisMetaManager] Task $taskId marked as FAILURE (updated=$updated)")
      } finally conn.close()
    } catch {
      case e: Exception =>
        logError(s"[DorisMetaManager] FAILURE transition failed with ${e.getClass.getSimpleName}")
    }
  }

  /**
   * close metadata manager
   */
  def close(): Unit = {
    connection.close()
    logInfo("[DorisMetaManager] MetaManager closed")
  }
}
