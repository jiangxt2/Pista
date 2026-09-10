package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Job-level metrics
 *
 * @param metricType    Metric type, fixed as "job"
 * @param appId         Application ID
 * @param appName       Application Name
 * @param jobId         Job ID
 * @param jobGroup     Job Group (typically SQL execution ID)
 * @param status        Status: RUNNING, SUCCEEDED, FAILED
 * @param startTime     Start timestamp (milliseconds Unix timestamp)
 * @param endTime       End timestamp (milliseconds Unix timestamp), SET TO RUNNING when -1
 * @param durationMs Configuration for execution duration (milliseconds), SET TO -1 when RUNNING.
 * @param numStages     Stage number
 * @param numTasks      Task number
 * @param collectTimestamp  Collection timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class JobMetrics(
  metricType: String = "job",
  appId: String,
  appName: String,
  jobId: Int,
  jobGroup: String,
  status: String,
  startTime: Long,
  endTime: Long,
  durationMs: Long,
  numStages: Int,
  numTasks: Int,
  collectTimestamp: String
) extends Metrics

object JobMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: JobMetrics): String = mapper.writeValueAsString(metrics)
}
