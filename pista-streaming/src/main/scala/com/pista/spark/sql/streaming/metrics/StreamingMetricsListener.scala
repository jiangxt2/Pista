package com.pista.spark.sql.streaming.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * Streaming query metrics listener
 *
 * Listen to progress, startup, and termination events of streaming queries to collect key metrics.
 *
 */
class StreamingMetricsListener extends StreamingQueryListener with Logging {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logInfo(s"Streaming query started: id=${event.id}, runId=${event.runId}, name=${event.name}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress

    // Base information
    val queryId = progress.id
    val queryName = progress.name
    val batchId = progress.batchId
    val timestamp = progress.timestamp

    // Process timeline metrics
    val batchDurationMs = Option(progress.durationMs.get("addBatch")).map(_.toLong)
    val triggerExecution = Option(progress.durationMs.get("triggerExecution")).map(_.toLong)
//    val latencyMs = Option(progress.durationMs.get("latency")).map(_.toLong)

    // Input metrics
    val inputRowsPerSecond = progress.inputRowsPerSecond
    val processedRowsPerSecond = progress.processedRowsPerSecond
    val numInputRows = progress.numInputRows

    // Status Metrics
    val stateOperators = progress.stateOperators
    val numStateStores = stateOperators.length
    val totalStateMemoryUsed = stateOperators.map(_.memoryUsedBytes).sum

    // output metrics
    val sink = progress.sink

    logInfo(
      s"""Streaming query progress:
         |  Query: $queryName (id=$queryId)
         |  Batch: $batchId @ $timestamp
         |  Duration: ${batchDurationMs.getOrElse("N/A")}ms (trigger=${triggerExecution.getOrElse("N/A")}ms)
         |  Input: $numInputRows rows ($inputRowsPerSecond rows/s)
         |  Processed: $processedRowsPerSecond rows/s
         |  State: $numStateStores stores, ${totalStateMemoryUsed / 1024 / 1024}MB memory
         |  Sink: ${sink.description}
         |""".stripMargin
    )

    // Detect slow batches
    batchDurationMs.foreach { duration =>
      if (duration > 60000) { // exceeds 1 minute
        logWarning(s"Slow batch detected: batchId=$batchId, duration=${duration}ms")
      }
    }

    // Detect Backpressure
    if (inputRowsPerSecond > processedRowsPerSecond * 1.5) {
      logWarning(
        s"Backpressure detected: input=$inputRowsPerSecond rows/s, " +
          s"processed=$processedRowsPerSecond rows/s"
      )
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val queryId = event.id
    val runId = event.runId

    event.exception match {
      case Some(ex) =>
        logError(s"Streaming query terminated with error: id=$queryId, runId=$runId, error=$ex")
      case None =>
        logInfo(s"Streaming query terminated normally: id=$queryId, runId=$runId")
    }
  }
}

/**
 * Streaming Metrics Manager
 */
object StreamingMetricsManager extends Logging {

  private var listener: Option[StreamingMetricsListener] = None

  /**
   * Initialize stream metrics collection
   */
  def initialize(spark: SparkSession): Unit = {
    if (listener.isEmpty) {
      val metricsListener = new StreamingMetricsListener()
      spark.streams.addListener(metricsListener)
      listener = Some(metricsListener)
      logInfo("StreamingMetricsListener initialized")
    } else {
      logWarning("StreamingMetricsListener already initialized")
    }
  }

  /**
   * remove listener
   */
  def shutdown(spark: SparkSession): Unit = {
    listener.foreach { l =>
      spark.streams.removeListener(l)
      listener = None
      logInfo("StreamingMetricsListener removed")
    }
  }
}
