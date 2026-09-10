package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Executor resource monitoring metrics
 *
 * @param metricType              Metric type, fixed as "executor"
 * @param appId                   Application ID
 * @param executorId              Executor ID
 * @param host                    Host Name
 * @param jvmHeapMemory JVM Heap Memory (Bytes)
 * @param jvmOffHeapMemory      JVM Non-Heap Memory (Metaspace, CodeCache, etc., bytes)
 * @param onHeapExecutionMemory Spark Heap Execution Memory (bytes)
 * @param onHeapStorageMemory    On-Heap Storage Memory in Spark (bytes)
 * @param onHeapUnifiedMemory    Spark Unified Memory Heap (bytes)
 * @param offHeapExecutionMemory Spark Off-Heap Execution Memory (bytes)
 * @param offHeapStorageMemory    Off-heap storage memory in bytes for Spark
 * @param offHeapUnifiedMemory Spark Off-Heap Unified Memory (bytes)
 * @param minorGCCount            Young GC Column Counts
 * @param minorGCTime             Young GC Time (milliseconds)
 * @param majorGCCount            Old GC count
 * @param majorGCTime             Old GC Time (milliseconds)
 * @param directPoolMemory        Direct Buffer Pool (Bytes)
 * @param mappedPoolMemory        Mapped Buffer Pool (Bytes)
 * @param activeTasks             active tasks Task number of active tasks
 * @param completedTasks          completed tasks Task number of completed tasks
 * @param failedTasks             Number of Failed Tasks
 * @param collectTimestamp             Collection timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class ExecutorMetrics(
  metricType: String = "executor",
  appId: String,
  executorId: String,
  host: String,

  // JVM memory
  jvmHeapMemory: Long,
  jvmOffHeapMemory: Long,

  // Spark Internal Memory
  onHeapExecutionMemory: Long,
  onHeapStorageMemory: Long,
  onHeapUnifiedMemory: Long,

  // Spark external memory
  offHeapExecutionMemory: Long,
  offHeapStorageMemory: Long,
  offHeapUnifiedMemory: Long,

  // GC
  minorGCCount: Long,
  minorGCTime: Long,
  majorGCCount: Long,
  majorGCTime: Long,

  // other
  directPoolMemory: Long,
  mappedPoolMemory: Long,

  // Task statistics
  activeTasks: Int,
  completedTasks: Int,
  failedTasks: Int,

  collectTimestamp: String
) extends Metrics

object ExecutorMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: ExecutorMetrics): String = mapper.writeValueAsString(metrics)
}
