package com.pista.spark.sql.streaming.metrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Streaming Metrics Listener Test
 *
 */
class StreamingMetricsListenerSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("StreamingMetricsListenerSuite")
    .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ==================== StreamingMetricsListener Unit Test ====================
  // Note: The constructors for QueryStartedEvent/QueryTerminatedEvent are private,
  // Unable to directly construct a mock event; integration tests cover this.

  // ==================== StreamingMetricsManager Test ====================

  test("initialize should register a listener") {
    // Ensure clean state
    StreamingMetricsManager.shutdown(spark)

    StreamingMetricsManager.initialize(spark)

    // Reinitialization should not throw an exception
    StreamingMetricsManager.initialize(spark)

    // Clean
    StreamingMetricsManager.shutdown(spark)
  }

  test("shutdown should remove the listener") {
    StreamingMetricsManager.initialize(spark)
    StreamingMetricsManager.shutdown(spark)

    // Shutdown attempts should not throw an error.
    StreamingMetricsManager.shutdown(spark)
  }

  test("initialize after shutdown and initialize again should work properly") {
    StreamingMetricsManager.initialize(spark)
    StreamingMetricsManager.shutdown(spark)
    StreamingMetricsManager.initialize(spark)
    StreamingMetricsManager.shutdown(spark)
  }

  // ==================== Integration Testing: Trigger Real Events Through Rate Source ====================

  test("listener should receive stream query event from the rate source") {
    val events = new java.util.concurrent.CopyOnWriteArrayList[String]()

    val testListener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit =
        events.add("started")

      override def onQueryProgress(event: QueryProgressEvent): Unit =
        events.add("progress")

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit =
        events.add("terminated")
    }

    spark.streams.addListener(testListener)

    try {
      val query = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
        .writeStream
        .format("memory")
        .queryName("metrics_listener_test")
        .outputMode("append")
        .start()

      // Wait for at least one batch to complete
      query.processAllAvailable()
      Thread.sleep(500)
      query.stop()
      query.awaitTermination()

      assert(events.contains("started"), "Received the started event")
      assert(events.contains("terminated"), "Received the terminated event")
    } finally {
      spark.streams.removeListener(testListener)
    }
  }
}
