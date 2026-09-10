package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Data skew detection metrics
 *
 * @param metricType           Type of metric, fixed as "data_skew"
 * @param appId                Application ID
 * @param stageId              Stage ID
 * @param stageAttemptId       Stage Attempt ID
 * @param isSkewed             isSkewed detected
 * @param skewThreshold       Skew threshold value (e.g., 3.0)
 * @param partitionCount      Number of partitions
 * @param durationMin          Minimum execution duration (milliseconds)
 * @param durationMax          Maximum execution duration (milliseconds)
 * @param durationMedian       Median execution duration (milliseconds)
 * @param durationSkewRatio    duration skew ratio (maximum / median)max / median)
 * @param skewedPartitions TopN (JSON format)
 * @param dataSizeMin          Minimum data size value (bytes, approximate estimation)
 * @param dataSizeMax          Maximum data size (bytes, approximate estimation)
 * @param dataSizeMedian       Median data size (bytes, approximate estimate)
 * @param dataSizeSkewRatio    Data skew ratio (max / median, approximate estimation)
 * @param recommendation       Optimization recommendations (repartition/salting/broadcast_join/default)
 * @param collectTimestamp          Collection Timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class DataSkewMetrics(
  metricType: String = "data_skew",
  appId: String,
  stageId: Int,
  stageAttemptId: Int,

  // Shrinkage detection
  isSkewed: Boolean,
  skewThreshold: Double,

  // Partition-level statistics
  partitionCount: Int,
  durationMin: Long,
  durationMax: Long,
  durationMedian: Long,
  durationSkewRatio: Double,

  // TopN skewed partition
  skewedPartitions: String,  // JSON: [{partition_id, size, duration}]

  // Data Skew (Approximate Estimation)
  dataSizeMin: Long,
  dataSizeMax: Long,
  dataSizeMedian: Long,
  dataSizeSkewRatio: Double,

  // Optimize recommendations
  recommendation: String,

  collectTimestamp: String
) extends Metrics

object DataSkewMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: DataSkewMetrics): String = mapper.writeValueAsString(metrics)
}
