package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.model.{ExecutorMetrics, Metrics}
import org.apache.spark.executor.{ExecutorMetrics => SparkExecutorMetrics}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Executor Aggregator Metrics
 *
 * aggregate ExecutorMetricsUpdate events and TaskEnd generate ExecutorMetrics by aggregating ExecutorMetricsUpdate events and TaskEnd events. ExecutorMetrics.
 *
 */
class ExecutorAggregator {

  // executorId -> (host, latestMetrics)
  private val executorMetrics = new ConcurrentHashMap[String, (String, SparkExecutorMetrics)]()

  // executorId -> (activeTasks, completedTasks, failedTasks)
  private val taskStats = new ConcurrentHashMap[String, (Int, Int, Int)]()

  /**
   * Update Executor metrics
   *
   * @param executorId Executor ID
   * @param host       Host Name
   * @param metrics    Spark ExecutorMetrics
   */
  def updateMetrics(executorId: String, host: String, metrics: SparkExecutorMetrics): Unit = {
    executorMetrics.put(executorId, (host, metrics))
  }

  /**
   * Record the start of Task*
   *
   * @param executorId Executor ID
   */
  def recordTaskStart(executorId: String): Unit = {
    taskStats.compute(executorId, (_, current) => {
      val (active, completed, failed) = Option(current).getOrElse((0, 0, 0))
      (active + 1, completed, failed)
    })
  }

  /**
   * logging task end Task end
   *
   * @param executorId Executor ID
   * @param success    Successful?
   */
  def recordTaskEnd(executorId: String, success: Boolean): Unit = {
    taskStats.compute(executorId, (_, current) => {
      val (active, completed, failed) = Option(current).getOrElse((0, 0, 0))
      if (success) {
        (active - 1, completed + 1, failed)
      } else {
        (active - 1, completed, failed + 1)
      }
    })
  }

  /**
   * Get snapshot of metrics for all Executors*
   *
   * @param appId Application ID
   * @return Executor Metrics List
   */
  def snapshot(appId: String): Seq[ExecutorMetrics] = {
    executorMetrics.asScala.map { case (executorId, (host, metrics)) =>
      val (active, completed, failed) = Option(taskStats.get(executorId)).getOrElse((0, 0, 0))

      ExecutorMetrics(
        appId = appId,
        executorId = executorId,
        host = host,
        jvmHeapMemory = metrics.getMetricValue("JVMHeapMemory"),
        jvmOffHeapMemory = metrics.getMetricValue("JVMOffHeapMemory"),
        onHeapExecutionMemory = metrics.getMetricValue("OnHeapExecutionMemory"),
        onHeapStorageMemory = metrics.getMetricValue("OnHeapStorageMemory"),
        onHeapUnifiedMemory = metrics.getMetricValue("OnHeapUnifiedMemory"),
        offHeapExecutionMemory = metrics.getMetricValue("OffHeapExecutionMemory"),
        offHeapStorageMemory = metrics.getMetricValue("OffHeapStorageMemory"),
        offHeapUnifiedMemory = metrics.getMetricValue("OffHeapUnifiedMemory"),
        minorGCCount = metrics.getMetricValue("MinorGCCount"),
        minorGCTime = metrics.getMetricValue("MinorGCTime"),
        majorGCCount = metrics.getMetricValue("MajorGCCount"),
        majorGCTime = metrics.getMetricValue("MajorGCTime"),
        directPoolMemory = metrics.getMetricValue("DirectPoolMemory"),
        mappedPoolMemory = metrics.getMetricValue("MappedPoolMemory"),
        activeTasks = active,
        completedTasks = completed,
        failedTasks = failed,
        collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
      )
    }.toSeq
  }

  /**
   * Clear data for the specified Executor*
   *
   * @param executorId Executor ID
   */
  def clear(executorId: String): Unit = {
    executorMetrics.remove(executorId)
    taskStats.remove(executorId)
  }
}
