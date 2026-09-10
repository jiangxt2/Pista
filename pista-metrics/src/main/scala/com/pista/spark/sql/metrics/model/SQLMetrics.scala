package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * SQL execution-level metrics
 *
 * @param metricType    Metric type, fixed as "sql"
 * @param appId         Application ID
 * @param executionId   SQL Execution ID (QueryExecution.id)
 * @param sqlText       SQL Original Text
 * @param status        Execution status (SUCCESS/FAILED)
 * @param errorMessage Error message (empty string for success)
 * @param startTime     Start Time (milliseconds Unix timestamp)
 * @param endTime       End time (milliseconds Unix timestamp)
 * @param durationMs    Duration in milliseconds during execution.
 * @param collectTimestamp  Collection Timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class SQLMetrics(
  metricType: String = "sql",
  appId: String,
  executionId: Long,
  sqlText: String,
  status: String,
  errorMessage: String,
  startTime: Long,
  endTime: Long,
  durationMs: Long,
  collectTimestamp: String
) extends Metrics

object SQLMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: SQLMetrics): String = mapper.writeValueAsString(metrics)
}
