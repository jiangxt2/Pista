package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Task Aggregates Metrics
 *
 * Aggregates task metrics by (stageId, stageAttemptId).
 *
 * @param metricType    Type of metric, fixed as "task_agg"
 * @param appId         Application ID
 * @param stageId       Stage ID
 * @param stageAttemptId Stage stageAttemptId Attempt ID
 * @param taskCount    Task Count
 * @param durationMin   Minimum execution duration (milliseconds)
 * @param durationMax   maximum execution duration (milliseconds)
 * @param durationAvg   Average execution duration (milliseconds)
 * @param durationP50   P50 duration
 * @param durationP95   P95 execution duration (milliseconds)
 * @param durationP99   P99 execution duration (milliseconds)
 * @param skewRatio     Skew ratio (max / p50)
 * @param collectTimestamp  Collection timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class TaskAggMetrics(
  metricType: String = "task_agg",
  appId: String,
  stageId: Int,
  stageAttemptId: Int,
  taskCount: Int,
  durationMin: Long,
  durationMax: Long,
  durationAvg: Long,
  durationP50: Long,
  durationP95: Long,
  durationP99: Long,
  skewRatio: Double,
  collectTimestamp: String
) extends Metrics

object TaskAggMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: TaskAggMetrics): String = mapper.writeValueAsString(metrics)
}
