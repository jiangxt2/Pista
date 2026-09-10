package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Stage stage metrics
 *
 * @param metricType        Type of metric, fixed as "stage"
 * @param appId             Application ID
 * @param stageId           Stage ID
 * @param stageAttemptId    Stage Attempt ID
 * @param jobId             Job ID belonging to the job (AQE dynamically added Stages may be -1)
 * @param status            Status: COMPLETED, FAILED
 * @param startTime         Start timestamp (milliseconds Unix timestamp)
 * @param endTime           End timestamp (milliseconds Unix timestamp)
 * @param durationMs        Duration in milliseconds during execution.
 * @param numTasks          Task Count
 * @param inputBytesBytes  Input byte sequence
 * @param inputRecords      Number of input records
 * @param outputBytes      outputBytes: bytes
 * @param outputRecords     Number of records produced
 * @param shuffleReadBytes  Shuffle number of bytes read
 * @param shuffleWriteBytes Write Shuffle Bytes Written
 * @param executorRunTime   Executor Runtime (milliseconds)
 * @param executorCpuTime Executor CPU Time (nanoseconds)
 * @param jvmGcTime         JVM GC Time (milliseconds)
 * @param collectTimestamp      Collection timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class StageMetrics(
  metricType: String = "stage",
  appId: String,
  stageId: Int,
  stageAttemptId: Int,
  jobId: Int,
  status: String,
  startTime: Long,
  endTime: Long,
  durationMs: Long,
  numTasks: Int,
  inputBytes: Long,
  inputRecords: Long,
  outputBytes: Long,
  outputRecords: Long,
  shuffleReadBytes: Long,
  shuffleWriteBytes: Long,
  executorRunTime: Long,
  executorCpuTime: Long,
  jvmGcTime: Long,
  collectTimestamp: String
) extends Metrics

object StageMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: StageMetrics): String = mapper.writeValueAsString(metrics)
}
