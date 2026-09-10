package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 **error diagnosis metrics**
 *
 * @param metricType       Type of metric, fixed as "error"
 * @param appId            Application ID
 * @param errorType        Error Type (SQL/JOB/STAGE/TASK)
 * @param executionId      SQL Execution ID (at the SQL level)
 * @param sqlHash          SQL Hash Value (SQL Level)
 * @param jobId            Job ID(Job/Stage/Task level)
 * @param stageId          Stage ID (Stage/Task Level)
 * @param stageAttemptId   Stage Attempt ID (Stage/Task Level)
 * @param taskId           Task ID(Task level)
 * @param taskAttemptId    Task Attempt ID (Task level)
 * @param errorMessage     Error Message
 * @param errorClass      Error class name (provided only for Task-level ExceptionFailure)
 * @param stackTrace      Stack trace (provided for Task-level ExceptionFailure failures)
 * @param executorId       Executor ID (Task Level)
 * @param host             task host name
 * @param failureReason    failure reason
 * @param retryCount      Number of retries
 * @param failureTime      Failure timestamp (milliseconds)
 * @param collectTimestamp      Collection Timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class ErrorMetrics(
  metricType: String = "error",
  appId: String,
  errorType: String,  // "SQL", "JOB", "STAGE", "TASK"

  // SQL level error
  executionId: Option[Long],
  sqlHash: Option[String],

  // Job/Stage/Task Level Error
  jobId: Option[Int],
  stageId: Option[Int],
  stageAttemptId: Option[Int],
  taskId: Option[Long],
  taskAttemptId: Option[Int],

  // Error details
  errorMessage: String,
  errorClass: Option[String],  // Only provided by a Task-level ExceptionFailure
  stackTrace: Option[String],  // Only provides a complete stack for an ExceptionFailure at the Task level

  // Failure Location
  executorId: Option[String],
  host: Option[String],

  // Failure statistics
  failureReason: String,
  retryCount: Int,

  // Time information
  failureTime: Long,
  collectTimestamp: String
) extends Metrics

object ErrorMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: ErrorMetrics): String = mapper.writeValueAsString(metrics)
}
