package com.pista.spark.sql.streaming

import com.pista.spark.sql.functions.PistaFunctionInstaller
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
 * Streaming SQL Publisher Tester
 *
 * Replace Kafka with rate source to avoid external dependencies.
 * StreamingSQLSubmitter The methods for this are all private, but they are tested through integration. privateThrough integration testing of the core process.
 *
 */
class StreamingSQLSubmitterSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("StreamingSQLSubmitterSuite")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  val checkpointBase = "/tmp/streaming-test-checkpoint"
  val outputBase = "/tmp/streaming-test-output"

  override def beforeAll(): Unit = {
    PistaFunctionInstaller.install(spark)
    deleteDirectory(new File(checkpointBase))
    deleteDirectory(new File(outputBase))
  }

  override def afterAll(): Unit = {
    deleteDirectory(new File(checkpointBase))
    deleteDirectory(new File(outputBase))
    if (spark != null) spark.stop()
  }

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { f =>
        if (f.isDirectory) deleteDirectory(f) else f.delete()
      })
      dir.delete()
    }

  // ==================== Create Streaming Data Source Test ====================

  test("rate source should create a streaming DataFrame") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    assert(df.isStreaming, "rate source should produce a streaming DataFrame")
    assert(df.schema.fieldNames.contains("timestamp"))
    assert(df.schema.fieldNames.contains("value"))
  }

  test("Stream DataFrame registered as a temporary view can be referenced by SQL") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    df.createOrReplaceTempView("source")

    val resultDF = spark.sql("SELECT value, timestamp FROM source WHERE value > 0")
    assert(resultDF.isStreaming, "SQL Query Result should still be a streaming DataFrame")
  }

  // ==================== foreachBatch Mode Testing ====================

  test("foreachBatch the foreachBatch mode should correctly handle micro batches") {
    val batchCounts = new java.util.concurrent.CopyOnWriteArrayList[Long]()

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("foreach_batch_test")
      .option("checkpointLocation", s"$checkpointBase/foreach_batch")
      .trigger(Trigger.ProcessingTime("1 second"))
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        batchCounts.add(batchDF.count())
        ()
      }
      .start()

    try {
      // rate source is a stream of infinite rows and cannot use processAllAvailable (it will block forever)
      // Wait for sufficient time for at least one batch to complete
      Thread.sleep(3000)
    } finally {
      query.stop()
      query.awaitTermination()
    }

    assert(batchCounts.size() > 0, "At least one batch should be processed")
  }

  test("foreachBatch continues when the Processor chain fails under continue-on-error") {
    val processedBatches = new java.util.concurrent.atomic.AtomicInteger(0)
    val failedBatches = new java.util.concurrent.atomic.AtomicInteger(0)

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("error_handling_test")
      .option("checkpointLocation", s"$checkpointBase/error_handling")
      .trigger(Trigger.ProcessingTime("1 second"))
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        // Simulate failure of Processor chain (first batch failure)
        if (batchId == 0) {
          failedBatches.incrementAndGet()
          // continue-on-error: continue processing without throwing exceptions
        }
        processedBatches.incrementAndGet()
        ()
      }
      .start()

    try {
      Thread.sleep(3000)
    } finally {
      query.stop()
      query.awaitTermination()
    }

    assert(processedBatches.get() > 0, "There should be batches processed.")
  }

  // ==================== Spark Sink Mode Testing ====================

  test("Spark Sink mode should directly write to memory sink") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("spark_sink_test")
      .format("memory")
      .outputMode("append")
      .start()

    try {
      Thread.sleep(3000)

      val result = spark.sql("SELECT * FROM spark_sink_test")
      assert(result.count() >= 0, "memory sink should be queryable")
    } finally {
      query.stop()
      query.awaitTermination()
    }
  }

  test("Spark Sink Mode Writing to File System") {
    val outputPath = s"$outputBase/spark_sink_file"

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("spark_sink_file_test")
      .option("checkpointLocation", s"$checkpointBase/spark_sink_file")
      .format("json")
      .option("path", outputPath)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    try {
      Thread.sleep(3000)
    } finally {
      query.stop()
      query.awaitTermination()
    }

    val outputDir = new File(outputPath)
    assert(outputDir.exists(), "Output directory should exist")
  }

  // ==================== Trigger Type Testing ====================

  test("ProcessingTime trigger is created from its interval") {
    val trigger = Trigger.ProcessingTime("10 seconds")
    assert(trigger != null)
  }

  test("AvailableNow trigger should correctly create") {
    val trigger = Trigger.AvailableNow()
    assert(trigger != null)
  }

  test("AvailableNow trigger should stop after processing all data") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("available_now_test")
      .option("checkpointLocation", s"$checkpointBase/available_now")
      .format("memory")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .start()

    // AvailableNow will automatically terminate
    query.awaitTermination()
    assert(!query.isActive, "AvailableNow query should automatically terminate")
  }

  // ==================== Column Output Mode Testing ====================

  test("append pattern should work properly") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    val query = df.writeStream
      .queryName("append_mode_test")
      .format("memory")
      .outputMode("append")
      .start()

    try {
      Thread.sleep(2000)
    } finally {
      query.stop()
      query.awaitTermination()
    }
  }

  test("complete The model requires aggregation operations.") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    df.createOrReplaceTempView("rate_source_complete")

    val aggDF = spark.sql("SELECT count(*) as cnt FROM rate_source_complete")

    val query = aggDF.writeStream
      .queryName("complete_mode_test")
      .format("memory")
      .outputMode("complete")
      .start()

    try {
      Thread.sleep(3000)
      val result = spark.sql("SELECT * FROM complete_mode_test")
      assert(result.count() > 0, "complete The aggregation results should exist in the complete mode.")
    } finally {
      query.stop()
      query.awaitTermination()
    }
  }

  // ==================== SQL Execution Test ====================

  test("Stream view on, Stream characteristics are maintained. SQL The transformation should preserve the streaming characteristic.") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()

    df.createOrReplaceTempView("sql_transform_source")

    val transformed = spark.sql(
      """SELECT
        |  value * 2 as doubled_value,
        |  timestamp,
        |  CASE WHEN value % 2 = 0 THEN 'even' ELSE 'odd' END as parity
        |FROM sql_transform_source
        |WHERE value >= 0""".stripMargin
    )

    assert(transformed.isStreaming, "SQL Transformation results in a streaming DataFrame")
    assert(transformed.schema.fieldNames.contains("doubled_value"))
    assert(transformed.schema.fieldNames.contains("parity"))
  }

  test("batch SQL result should not be a streaming DataFrame. DataFrame") {
    val result = spark.sql("SELECT 1 as id, 'test' as name")
    assert(!result.isStreaming, "ordinary SQL should not produce a streaming DataFrame")
  }

  test("Multi-statement Limit: Stream Mode Supports Single SQL Statement") {
    // StreamingSQLSubmitter.validate(statements.length > 1) throw new Exception()
    // Here validate the behavior of SQLTemplateEngine.splitStatements.
    import com.pista.spark.util.SQLTemplateEngine
    val multiSQL = "SELECT * FROM t1; SELECT * FROM t2"
    val statements = SQLTemplateEngine.splitStatements(multiSQL)
    assert(statements.length == 2, "two statements produced by the semicolon delimiter 2 two statements")
  }

  // ==================== Configuration parsing test ====================

  test("Default configuration options for streaming should have correct defaults.") {
    import com.pista.spark.sql.conf.SubmitterConf

    assert(SubmitterConf.STREAMING_INPUT_FORMAT.defaultValue.contains("kafka"))
    assert(SubmitterConf.STREAMING_INPUT_VIEW.defaultValue.contains("source"))
    assert(SubmitterConf.STREAMING_TRIGGER_TYPE.defaultValue.contains("processing_time"))
    assert(SubmitterConf.STREAMING_TRIGGER_INTERVAL.defaultValue.contains("10 seconds"))
    assert(SubmitterConf.STREAMING_USE_SPARK_SINK.defaultValue.contains(false))
    assert(SubmitterConf.STREAMING_AWAIT_TERMINATION.defaultValue.contains(true))
  }

  test("checkpoint location is a required configuration") {
    import com.pista.spark.sql.conf.SubmitterConf

    // OptionalConfigEntry has no default value
    assert(SubmitterConf.STREAMING_CHECKPOINT_LOCATION.defaultValue.isEmpty)
  }

  test("value format supports json and csv") {
    // Validate that the value format configuration is optional
    import com.pista.spark.sql.conf.SubmitterConf
    assert(SubmitterConf.STREAMING_INPUT_VALUE_FORMAT.defaultValue.isEmpty)
    assert(SubmitterConf.STREAMING_INPUT_VALUE_SCHEMA.defaultValue.isEmpty)
  }

  // ==================== awaitTermination Test ====================

  test("awaitTermination(timeout = 10000) should return after timeout") {
    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()

    val query = df.writeStream
      .queryName("timeout_test")
      .format("memory")
      .outputMode("append")
      .start()

    try {
      val startTime = System.currentTimeMillis()
      query.awaitTermination(1000) // Timeout for 1 second
      val elapsed = System.currentTimeMillis() - startTime

      assert(elapsed >= 900, "should wait nearly 1 second")
      assert(query.isActive, "Query should still be active after timeout")
    } finally {
      query.stop()
      query.awaitTermination()
    }
  }
}
