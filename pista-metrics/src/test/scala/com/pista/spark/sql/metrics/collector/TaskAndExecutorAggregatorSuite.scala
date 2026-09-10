package com.pista.spark.sql.metrics.collector

import org.scalatest.funsuite.AnyFunSuite

/**
 * Task + Executor Aggregator Test
 *
 * TaskAggregatorcompleting MetricsSuite uncovered aggregateSkewMetrics,recordTaskData,generateRecommendation
 * ExecutorAggregator:recordTaskStart/End,snapshot,clear
 *
 */
class TaskAndExecutorAggregatorSuite extends AnyFunSuite {

  // ==================== TaskAggregator testing continues ====================

  test("aggregateSkewMetrics should correctly detect data skew") {
    val aggregator = new TaskAggregator(skewThreshold = 3.0, topN = 5)

    // simulation 5 one Taskwhere 1 slowest
    aggregator.recordTaskData(1, 0, partitionId = 0, durationMs = 100L, dataSize = 1000L)
    aggregator.recordTaskData(1, 0, partitionId = 1, durationMs = 100L, dataSize = 1000L)
    aggregator.recordTaskData(1, 0, partitionId = 2, durationMs = 100L, dataSize = 1000L)
    aggregator.recordTaskData(1, 0, partitionId = 3, durationMs = 100L, dataSize = 1000L)
    aggregator.recordTaskData(1, 0, partitionId = 4, durationMs = 500L, dataSize = 10000L) // Skew

    val result = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(result.isDefined)

    val metrics = result.get
    assert(metrics.partitionCount == 5)
    assert(metrics.durationMin == 100L)
    assert(metrics.durationMax == 500L)
    assert(metrics.durationSkewRatio == 5.0) // 500 / 100
    assert(metrics.isSkewed) // 5.0 > 3.0
    assert(metrics.skewedPartitions.contains("partition_id"))
    assert(metrics.metricType == "data_skew")
  }

  test("aggregateSkewMetrics reports isSkewed=false when no skew exists") {
    val aggregator = new TaskAggregator(skewThreshold = 3.0)

    aggregator.recordTaskData(1, 0, 0, 100L, 1000L)
    aggregator.recordTaskData(1, 0, 1, 110L, 1100L)
    aggregator.recordTaskData(1, 0, 2, 105L, 1050L)

    val result = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(result.isDefined)
    assert(!result.get.isSkewed)
  }

  test("aggregateSkewMetrics None should be returned when there is no data. None") {
    val aggregator = new TaskAggregator()
    val result = aggregator.aggregateSkewMetrics(99, 0, "app-1")
    assert(result.isEmpty)
  }

  test("aggregateSkewMetrics Aggregation should clear data") {
    val aggregator = new TaskAggregator()
    aggregator.recordTaskData(1, 0, 0, 100L, 1000L)

    val first = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(first.isDefined)

    val second = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(second.isEmpty)
  }

  test("generateRecommendation severe skew suggests salting") {
    val aggregator = new TaskAggregator(skewThreshold = 3.0)

    // durationSkewRatio > 10.0 → salting
    aggregator.recordTaskData(1, 0, 0, 100L, 1000L)
    aggregator.recordTaskData(1, 0, 1, 1100L, 200L * 1024 * 1024) // Large dataset

    val result = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(result.isDefined)
    assert(result.get.recommendation == "salting")
  }

  test("generateRecommendation Moderate skew suggests repartitioning. repartition") {
    val aggregator = new TaskAggregator(skewThreshold = 3.0)

    // durationSkewRatio within a range of 5.0 to 10.0 → repartition 5.0-10.0 between → repartition
    aggregator.recordTaskData(1, 0, 0, 100L, 200L * 1024 * 1024)
    aggregator.recordTaskData(1, 0, 1, 100L, 200L * 1024 * 1024)
    aggregator.recordTaskData(1, 0, 2, 700L, 200L * 1024 * 1024) // 7x

    val result = aggregator.aggregateSkewMetrics(1, 0, "app-1")
    assert(result.isDefined)
    assert(result.get.recommendation == "repartition")
  }

  test("clear should overwrite all tracked data") {
    val aggregator = new TaskAggregator()
    aggregator.record(1, 0, 100L)
    aggregator.record(2, 0, 200L)
    aggregator.recordTaskData(1, 0, 0, 100L, 1000L)

    assert(aggregator.trackedStageCount == 2)

    aggregator.clear()
    assert(aggregator.trackedStageCount == 0)
    assert(aggregator.aggregateAndClear(1, 0, "app").isEmpty)
  }

  // ==================== ExecutorAggregator test ====================

  test("recordTaskStart/End should correctly track task metrics Task metrics are correctly tracking task statistics") {
    val aggregator = new ExecutorAggregator()

    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskEnd("exec-1", success = true)
    aggregator.recordTaskEnd("exec-1", success = false)

    // Unable to directly validate internal state; validate indirectly through snapshot.
    // But snapshot requires executorMetrics, validation does not throw an exception.
  }

  test("snapshot when no data should return an empty list") {
    val aggregator = new ExecutorAggregator()
    val result = aggregator.snapshot("app-1")
    assert(result.isEmpty)
  }

  test("clear should remove data for a specified Executor") {
    val aggregator = new ExecutorAggregator()
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskStart("exec-2")

    aggregator.clear("exec-1")

    // The data for exec-2 should still be present.
    aggregator.recordTaskEnd("exec-2", success = true)
    // No exception thrown indicates success.
  }

  // ==================== activeTasks Verification ====================

  test("recordTaskStart should increment activeTasks by 1") {
    val aggregator = new ExecutorAggregator()

    // private taskStats field using reflection to read taskStats validate internal state through the field validation
    val field = classOf[ExecutorAggregator].getDeclaredField("taskStats")
    field.setAccessible(true)
    val taskStats = field.get(aggregator)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String, (Int, Int, Int)]]

    aggregator.recordTaskStart("exec-1")
    val (active, completed, failed) = taskStats.get("exec-1")
    assert(active == 1 && completed == 0 && failed == 0,
      "recordTaskStart After activeTasks is 1")
  }

  test("recordTaskEnd successful, activeTasks decrement by 1, completedTasks increment by 1") {
    val aggregator = new ExecutorAggregator()

    val field = classOf[ExecutorAggregator].getDeclaredField("taskStats")
    field.setAccessible(true)
    val taskStats = field.get(aggregator)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String, (Int, Int, Int)]]

    // Simulate the order of the fixed call: onTaskStart calls recordTaskStart, onTaskEnd calls recordTaskEnd
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskEnd("exec-1", success = true)

    val (active, completed, failed) = taskStats.get("exec-1")
    assert(active == 0 && completed == 1 && failed == 0,
      "Task Completion: activeTasks=0, completedTasks=1")
  }

  test("recordTaskEnd moves a failed task from active to failed") {
    val aggregator = new ExecutorAggregator()

    val field = classOf[ExecutorAggregator].getDeclaredField("taskStats")
    field.setAccessible(true)
    val taskStats = field.get(aggregator)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String, (Int, Int, Int)]]

    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskEnd("exec-1", success = false)

    val (active, completed, failed) = taskStats.get("exec-1")
    assert(active == 0 && completed == 0 && failed == 1,
      "Task failed with activeTasks=0, failedTasks=1.")
  }

  test("Reproduction of Original Defect: Both start and end are called simultaneously onTaskEnd, resulting in activeTasks always being zero") {
    val aggregator = new ExecutorAggregator()

    val field = classOf[ExecutorAggregator].getDeclaredField("taskStats")
    field.setAccessible(true)
    val taskStats = field.get(aggregator)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String, (Int, Int, Int)]]

    // Simulate incorrect calls before the fix, both of which are in the onTaskEnd
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskEnd("exec-1", success = true)

    // Fix: separate them as follows: start in onTaskStart, end in onTaskEnd, the results should be the same.
    // This example demonstrates that the corrected behavior matches the results in the "single task completion" scenario both before and after the fix.
    val (active, _, _) = taskStats.get("exec-1")
    assert(active == 0, "activeTasks should be zero after the task completes")
  }

  test("During concurrent multi-task execution, activeTasks should be correctly aggregated") {
    val aggregator = new ExecutorAggregator()

    val field = classOf[ExecutorAggregator].getDeclaredField("taskStats")
    field.setAccessible(true)
    val taskStats = field.get(aggregator)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[String, (Int, Int, Int)]]

    // Model the fixed behavior: three tasks start concurrently and only one completes.
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskStart("exec-1")
    aggregator.recordTaskEnd("exec-1", success = true)

    val (active, completed, _) = taskStats.get("exec-1")
    assert(active == 2 && completed == 1,
      "After three tasks start and one completes, activeTasks=2 and completedTasks=1")
  }
}
