package com.pista.spark.sql.execution.checkpoint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * MaterializationManager usage example
 *
 */
object MaterializationManagerExample extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MaterializationManagerExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ========== Example 1: Basic Usage ==========
    logInfo("Example 1: basic usage")

    // Initialize MaterializationManager
    MaterializationManager.initialize(spark, "hdfs://namenode:8020/checkpoints")
    // Or use local path: MaterializationManager.initialize(spark, "file:///tmp/checkpoints")
    // Or use S3: MaterializationManager.initialize(spark, "s3://bucket/checkpoints")

    // Create test data.
    val df = Seq(
      (1, "Alice", 25),
      (2, "Bob", 30),
      (3, "Charlie", 35)
    ).toDF("id", "name", "age")

    // Create a materialization.
    val handle = MaterializationManager.checkpoint(df, eager = true)
    logInfo(s"Checkpoint created: ${handle.id}")

    // DataFrame after checkpointing
    val result = handle.dataFrame.filter($"age" > 25)
    result.show()

    // checkpoint to persist state manually
    handle.release()

    // ========== Example 2: Serializing Multiple Processors ==========
    logInfo("Example 2: materializing multiple processors")

    val rawDF = Seq(
      (1, "user1", 100),
      (2, "user2", 200),
      (3, "user3", 300)
    ).toDF("id", "user", "amount")

    // Processor 1: Data cleaning
    val cleaned = rawDF.filter($"amount" > 0)
    val handle1 = MaterializationManager.checkpoint(cleaned, eager = true)
    logInfo(s"Checkpoint 1: ${handle1.id}")

    // Processor 2Feature Engineering
    val features = handle1.dataFrame.withColumn("amount_squared", $"amount" * $"amount")
    val handle2 = MaterializationManager.checkpoint(features, eager = true)
    logInfo(s"Checkpoint 2: ${handle2.id}")

    // Processor 3: aggregation
    val aggregated = handle2.dataFrame.groupBy("user").sum("amount")
    val handle3 = MaterializationManager.checkpoint(aggregated, eager = true)
    logInfo(s"Checkpoint 3: ${handle3.id}")

    // View all active checkpoints
    logInfo(s"Active checkpoints: ${MaterializationManager.getCheckpointCount}")
    MaterializationManager.printStatus()

    // Release intermediate results (keep final result)
    handle1.release()
    handle2.release()
    logInfo(s"Active checkpoints after release: ${MaterializationManager.getCheckpointCount}")

    // Using the final result
    handle3.dataFrame.show()

    // Release final results
    handle3.release()

    // ========== Example 3: Iterative Algorithm ==========
    logInfo("Example 3: iterative algorithm")

    var iterDF = Seq((1, 100), (2, 200), (3, 300)).toDF("id", "value")

    for (i <- 1 to 5) {
      // Perform some computations during each iteration
      iterDF = iterDF.withColumn("value", $"value" * 1.1)

      // Checkpoint every 2 iterations.
      if (i % 2 == 0) {
        val handle = MaterializationManager.checkpoint(iterDF, eager = true)
        logInfo(s"Iteration $i: checkpoint ${handle.id}")
        iterDF = handle.dataFrame
      }
    }

    logInfo("Final result")
    iterDF.show()

    // Overwrite all checkpoints
    MaterializationManager.releaseAll()
    logInfo("All checkpoints released")

    // ========== Example 4: Branch Scenario ==========
    logInfo("Example 4: branch scenario")

    val baseDF = Seq(
      (1, "A", 100),
      (2, "B", 200),
      (3, "C", 300)
    ).toDF("id", "category", "value")

    // Checkpoint base data
    val baseHandle = MaterializationManager.checkpoint(baseDF, eager = true)
    logInfo(s"Base checkpoint: ${baseHandle.id}")

    // Branch 1: Aggregate by category
    val branch1 = baseHandle.dataFrame.groupBy("category").sum("value")
    branch1.show()

    // branch 2: filter high-value data
    val branch2 = baseHandle.dataFrame.filter($"value" > 150)
    branch2.show()

    // Branch 3: Compute statistical information
    val branch3 = baseHandle.dataFrame.describe()
    branch3.show()

    // checkpoint overwrite and persist metrics
    baseHandle.release()

    // ========== Example 5: Lazy Checkpoint ==========
    logInfo("Example 5: lazy checkpoint")

    val lazyDF = Seq((1, "X"), (2, "Y"), (3, "Z")).toDF("id", "label")

    // Create a lazy checkpoint (do not execute immediately)
    val lazyHandle = MaterializationManager.checkpoint(lazyDF, eager = false)
    logInfo(s"Lazy checkpoint created: ${lazyHandle.id}")

    // Data is materialized for the first action.
    logInfo("Triggering action")
    val count = lazyHandle.dataFrame.count()
    logInfo(s"Count: $count")

    // Release
    lazyHandle.release()

    // ========== Clean ==========
    logInfo("Cleanup")

    // Print final state
    MaterializationManager.printStatus()

    // Clean all resources (which will be automatically cleaned up when JVM shuts down)
    MaterializationManager.cleanup()

    spark.stop()
  }
}
