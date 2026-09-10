package com.pista.spark.sql.execution

import com.pista.spark.util.LogRedaction
import org.apache.spark.SparkThrowable

import scala.collection.mutable.ArrayBuffer

/**
 * Execution result of a single SQL statement*
 *
 */
case class ExecutionResult(
    sqlIndex: Int,
    sqlFingerprint: String,
    status: ExecutionStatus,
    errorClass: Option[String] = None,
    errorType: Option[String] = None,
    durationMs: Long = 0)

sealed trait ExecutionStatus
object ExecutionStatus {
  case object Success extends ExecutionStatus
  case object Failed extends ExecutionStatus
  case object Skipped extends ExecutionStatus
}

/**
 * job execution report
 */
class ExecutionReport {
  private val results = ArrayBuffer[ExecutionResult]()

  def addSuccess(sqlIndex: Int, sql: String, durationMs: Long): Unit = {
    results += ExecutionResult(
      sqlIndex,
      LogRedaction.fingerprint(sql),
      ExecutionStatus.Success,
      durationMs = durationMs)
  }

  def addFailure(sqlIndex: Int, sql: String, error: Throwable, durationMs: Long): Unit = {
    results += failureResult(sqlIndex, sql, error, durationMs)
  }

  def replaceWithFailure(
      sqlIndex: Int,
      sql: String,
      error: Throwable,
      durationMs: Long): Unit = {
    val replacement = failureResult(sqlIndex, sql, error, durationMs)
    val existing = results.indexWhere(_.sqlIndex == sqlIndex)
    if (existing >= 0) results.update(existing, replacement)
    else results += replacement
  }

  private def failureResult(
      sqlIndex: Int,
      sql: String,
      error: Throwable,
      durationMs: Long): ExecutionResult = {
    val errorClass = error match {
      case st: SparkThrowable =>
        Option(st.getErrorClass)
      case _ =>
        None
    }
    ExecutionResult(
      sqlIndex,
      LogRedaction.fingerprint(sql),
      ExecutionStatus.Failed,
      errorClass,
      Some(LogRedaction.exceptionName(error)),
      durationMs)
  }

  def addSkipped(sqlIndex: Int, sql: String): Unit = {
    results += ExecutionResult(
      sqlIndex,
      LogRedaction.fingerprint(sql),
      ExecutionStatus.Skipped)
  }

  def totalCount: Int = results.size
  def successCount: Int = results.count(_.status == ExecutionStatus.Success)
  def failureCount: Int = results.count(_.status == ExecutionStatus.Failed)
  def skippedCount: Int = results.count(_.status == ExecutionStatus.Skipped)

  def failures: Seq[ExecutionResult] = results.filter(_.status == ExecutionStatus.Failed).toSeq

  def hasFailures: Boolean = failureCount > 0

  /**
   * Generate a formatted report
   */
  def formatReport: String = {
    val sb = new StringBuilder
    sb.append("=" * 60).append("\n")
    sb.append("Pista Job Execution Report\n")
    sb.append("=" * 60).append("\n")
    sb.append(s"Total SQL statements: $totalCount\n")
    sb.append(s"Succeeded: $successCount\n")
    sb.append(s"Failed: $failureCount\n")
    sb.append(s"Skipped: $skippedCount\n")

    if (hasFailures) {
      sb.append("\nFailure details:\n")
      failures.foreach { result =>
        sb.append(s"  [${result.errorClass.getOrElse("UNKNOWN")}] ")
        sb.append(s"SQL statement ${result.sqlIndex} failed:\n")
        sb.append(s"    SQL fingerprint: ${result.sqlFingerprint}\n")
        sb.append(s"    Error type: ${result.errorType.getOrElse("UnknownError")}\n")
      }
    }

    sb.append("=" * 60).append("\n")
    sb.toString()
  }
}
