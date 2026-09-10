package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Shuffle Performance Metrics
 *
 * @param metricType                  Metric type, fixed as "shuffle"
 * @param appId                       Application ID
 * @param stageId                     Stage ID
 * @param stageAttemptId              Stage Attempt ID
 * @param shuffleWriteBytes           Shuffle number of bytes written
 * @param shuffleWriteRecords         Shuffle shuffleWriteRecords
 * @param shuffleWriteTimeNs          Shuffle Write Time (Nanos)
 * @param shuffleReadBytes            Shuffle Read Bytes
 * @param shuffleReadRecords          Shuffle number of read records
 * @param shuffleFetchWaitTimeMs      Shuffle Fetch Wait Time (Milliseconds)
 * @param shuffleRemoteBlocksFetched  remote Block quantity
 * @param shuffleLocalBlocksFetched   local Block quantity
 * @param shuffleSpillMemory          Shuffle Spill Memory (bytes)
 * @param shuffleSpillDisk            Shuffle Spill Disk (Bytes)
 * @param collectTimestamp                 Collection timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class ShuffleMetrics(
  metricType: String = "shuffle",
  appId: String,
  stageId: Int,
  stageAttemptId: Int,

  // Shuffle Write
  shuffleWriteBytes: Long,
  shuffleWriteRecords: Long,
  shuffleWriteTimeNs: Long,  // nanoseconds

  // Shuffle read
  shuffleReadBytes: Long,
  shuffleReadRecords: Long,
  shuffleFetchWaitTimeMs: Long,  // milliseconds
  shuffleRemoteBlocksFetched: Long,
  shuffleLocalBlocksFetched: Long,

  // Shuffle Overflow
  shuffleSpillMemory: Long,
  shuffleSpillDisk: Long,

  collectTimestamp: String
) extends Metrics

object ShuffleMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: ShuffleMetrics): String = mapper.writeValueAsString(metrics)
}
