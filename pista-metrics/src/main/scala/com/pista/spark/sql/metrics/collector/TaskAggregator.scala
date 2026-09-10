package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.model.{DataSkewMetrics, Metrics, TaskAggMetrics}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.collection.JavaConverters._

/**
 * Task aggregation aggregator
 *
 * Collects Task duration and data volume for each Stage, then computes aggregates when the Stage completes.
 * Use ConcurrentLinkedQueue to avoid write amplification issues.
 *
 */
class TaskAggregator(skewThreshold: Double = 3.0, topN: Int = 10) {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // (stageId, stageAttemptId) -> Task Execution Time Duration List
  private val taskDurations = new ConcurrentHashMap[(Int, Int), ConcurrentLinkedQueue[Long]]()

  // (stageId, stageAttemptId) -> Task data (partitionId, duration, size)partitionId, duration, size)
  private val taskData = new ConcurrentHashMap[(Int, Int), ConcurrentLinkedQueue[(Int, Long, Long)]]()

  /**
   * record task execution duration Task execution duration
   *
   * @param stageId        Stage ID
   * @param stageAttemptId Stage Attempt ID
   * @param durationMs     Duration in milliseconds during execution.
   */
  def record(stageId: Int, stageAttemptId: Int, durationMs: Long): Unit = {
    val key = (stageId, stageAttemptId)
    taskDurations
      .computeIfAbsent(key, _ => new ConcurrentLinkedQueue[Long]())
      .add(durationMs)
  }

  /**
   * Records task details for data-skew detection.
   *
   * @param stageId        Stage ID
   * @param stageAttemptId Stage Attempt ID
   * @param partitionId Column ID
   * @param durationMs     Duration in milliseconds during execution.
   * @param dataSize       Data size (bytes, approximate estimation)
   */
  def recordTaskData(stageId: Int, stageAttemptId: Int, partitionId: Int, durationMs: Long, dataSize: Long): Unit = {
    val key = (stageId, stageAttemptId)
    taskData
      .computeIfAbsent(key, _ => new ConcurrentLinkedQueue[(Int, Long, Long)]())
      .add((partitionId, durationMs, dataSize))
  }

  /**
   * Merge and clear task data for the specified stage attempt.
   *
   * @param stageId        Stage ID
   * @param stageAttemptId Stage Attempt ID
   * @param appId          Application ID (explicitly passed in to avoid null value)
   * @return aggregated Task metrics, returning null if no data exists Task metric, no data then return None
   */
  def aggregateAndClear(stageId: Int, stageAttemptId: Int, appId: String): Option[TaskAggMetrics] = {
    val key = (stageId, stageAttemptId)
    Option(taskDurations.remove(key)).flatMap { durations =>
      val durationList = durations.asScala.toSeq
      if (durationList.isEmpty) {
        None
      } else {
        val sorted = durationList.sorted
        val count = sorted.size
        val sum = sorted.sum
        val avg = sum / count
        val min = sorted.head
        val max = sorted.last
        val p50 = percentile(sorted, 0.50)
        val p95 = percentile(sorted, 0.95)
        val p99 = percentile(sorted, 0.99)
        val skewRatio = if (p50 > 0) max.toDouble / p50 else 0.0

        Some(TaskAggMetrics(
          appId = appId,
          stageId = stageId,
          stageAttemptId = stageAttemptId,
          taskCount = count,
          durationMin = min,
          durationMax = max,
          durationAvg = avg,
          durationP50 = p50,
          durationP95 = p95,
          durationP99 = p99,
          skewRatio = skewRatio,
          collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
        ))
      }
    }
  }

  /**
   * Aggregate data skew metrics and checkpoint the data.
   *
   * @param stageId        Stage ID
   * @param stageAttemptId Stage Attempt ID
   * @param appId          Application ID
   * @return data skew metric, return None if no data exists None
   */
  def aggregateSkewMetrics(stageId: Int, stageAttemptId: Int, appId: String): Option[DataSkewMetrics] = {
    val key = (stageId, stageAttemptId)
    Option(taskData.remove(key)).flatMap { data =>
      val dataList = data.asScala.toSeq
      if (dataList.isEmpty) {
        None
      } else {
        // Extract task duration and data size.
        val durations = dataList.map(_._2).sorted
        val sizes = dataList.map(_._3).sorted

        val durationMedian = percentile(durations, 0.50)
        val durationMin = durations.head
        val durationMax = durations.last
        val durationSkewRatio = if (durationMedian > 0) durationMax.toDouble / durationMedian else 0.0

        val dataSizeMedian = percentile(sizes, 0.50)
        val dataSizeMin = sizes.head
        val dataSizeMax = sizes.last
        val dataSizeSkewRatio = if (dataSizeMedian > 0) dataSizeMax.toDouble / dataSizeMedian else 0.0

        // Anomaly Detection
        val isSkewed = durationSkewRatio > skewThreshold

        // TopN skewed partition
        val topSkewed = dataList
          .sortBy(-_._2)  // by duration in descending order duration descending order
          .take(topN)
          .map { case (partitionId, duration, size) =>
            Map("partition_id" -> partitionId, "duration" -> duration, "size" -> size)
          }
        val skewedPartitionsJson = mapper.writeValueAsString(topSkewed)

        // Optimize recommendations
        val recommendation = generateRecommendation(durationSkewRatio, dataSizeSkewRatio, dataSizeMax)

        Some(DataSkewMetrics(
          appId = appId,
          stageId = stageId,
          stageAttemptId = stageAttemptId,
          isSkewed = isSkewed,
          skewThreshold = skewThreshold,
          partitionCount = dataList.size,
          durationMin = durationMin,
          durationMax = durationMax,
          durationMedian = durationMedian,
          durationSkewRatio = durationSkewRatio,
          skewedPartitions = skewedPartitionsJson,
          dataSizeMin = dataSizeMin,
          dataSizeMax = dataSizeMax,
          dataSizeMedian = dataSizeMedian,
          dataSizeSkewRatio = dataSizeSkewRatio,
          recommendation = recommendation,
          collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
        ))
      }
    }
  }

  /**
   * Generate optimization suggestions
   *
   * @param durationSkewRatio Execution skew ratio
   * @param dataSizeSkewRatio Data Skew Ratio
   * @param maxSize           maximum data size
   * @return Optimization recommendations
   */
  private def generateRecommendation(durationSkewRatio: Double, dataSizeSkewRatio: Double, maxSize: Long): String = {
    if (durationSkewRatio > 10.0) {
      "salting"  // severe skew, use salting
    } else if (durationSkewRatio > 5.0) {
      "repartition"  // Moderately skewed, repartition
    } else if (maxSize < 100 * 1024 * 1024) {
      "broadcast_join"  // broadcast-table join
    } else {
      "none"
    }
  }

  /**
   * Compute quantiles
   *
   * @param sorted Sorted data
   * @param p      Quantile (0.0 - 1.0)
   * @return percentile
   */
  private def percentile(sorted: Seq[Long], p: Double): Long = {
    if (sorted.isEmpty) 0L
    else {
      val index = math.min(((sorted.size - 1) * p).toInt, sorted.size - 1)
      sorted(index)
    }
  }

  /**
   * clear all data
   */
  def clear(): Unit = {
    taskDurations.clear()
    taskData.clear()
  }

  /**
   * Get the number of current tracked Stages
   */
  def trackedStageCount: Int = taskDurations.size()
}
