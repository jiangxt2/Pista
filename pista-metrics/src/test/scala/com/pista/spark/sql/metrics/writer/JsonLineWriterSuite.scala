package com.pista.spark.sql.metrics.writer

import com.pista.spark.sql.metrics.model._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * JsonLineWriter Writing test
 *
 * overwrites: startup/shutdown lifecycle, batch write, write count, output directory auto creation
 * Complements the MetricsSuite integration coverage with lifecycle and boundary cases.
 *
 */
class JsonLineWriterSuite extends AnyFunSuite with BeforeAndAfterEach {

  val outputDir = "/tmp/jsonline-writer-suite-test"

  override def afterEach(): Unit = deleteDirectory(new File(outputDir))

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { f =>
        if (f.isDirectory) deleteDirectory(f) else f.delete()
      })
      dir.delete()
    }

  private def createQueue(capacity: Int = 100): MetricsQueue[Metrics] =
    new MetricsQueue[Metrics](capacity, DropPolicy.DropOldest)

  private def sampleJobMetrics(jobId: Int = 1): JobMetrics = JobMetrics(
    appId = "test-app", appName = "Test", jobId = jobId, jobGroup = "",
    status = "SUCCEEDED", startTime = 1000L, endTime = 2000L, durationMs = 1000L,
    numStages = 1, numTasks = 5, collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
  )

  test("start should automatically create the output directory") {
    val queue = createQueue()
    val writer = new JsonLineWriter(outputDir, queue, batchSize = 10, flushIntervalMs = 100)

    assert(!Files.exists(Paths.get(outputDir)))
    writer.start()
    assert(Files.exists(Paths.get(outputDir)))
    writer.stop()
  }

  test("stop should flush remaining data") {
    val queue = createQueue()
    val writer = new JsonLineWriter(outputDir, queue, batchSize = 100, flushIntervalMs = 60000)
    writer.start()

    // Enqueue but do not wait for automatic flush
    queue.offer(sampleJobMetrics(1))
    queue.offer(sampleJobMetrics(2))

    // Stop triggers a flushAll.
    writer.stop()

    val stats = writer.stats
    assert(stats.writeCount == 2, s"After stopping, all data should be flushed, actual ${stats.writeCount}")
    assert(stats.errorCount == 0)
  }

  test("stats should correctly reflect write counts and error counts") {
    val queue = createQueue()
    val writer = new JsonLineWriter(outputDir, queue, batchSize = 10, flushIntervalMs = 50)
    writer.start()

    (1 to 5).foreach(i => queue.offer(sampleJobMetrics(i)))
    Thread.sleep(300)

    writer.stop()

    val stats = writer.stats
    assert(stats.writeCount == 5)
    assert(stats.errorCount == 0)
    assert(stats.queueSize == 0)
  }

  test("different metrics should be written to different files metricType should write into different files") {
    val queue = createQueue()
    val writer = new JsonLineWriter(outputDir, queue, batchSize = 10, flushIntervalMs = 50)
    writer.start()

    queue.offer(sampleJobMetrics())
    queue.offer(StageMetrics(
      appId = "test-app", stageId = 1, stageAttemptId = 0, jobId = 1,
      status = "COMPLETED", startTime = 1000L, endTime = 2000L, durationMs = 1000L,
      numTasks = 5, inputBytes = 0L, inputRecords = 0L, outputBytes = 0L,
      outputRecords = 0L, shuffleReadBytes = 0L, shuffleWriteBytes = 0L,
      executorRunTime = 0L, executorCpuTime = 0L, jvmGcTime = 0L,
      collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
    ))

    Thread.sleep(300)
    writer.stop()

    assert(Files.exists(Paths.get(outputDir, "job_metrics.jsonl")))
    assert(Files.exists(Paths.get(outputDir, "stage_metrics.jsonl")))
  }

  test("The JSONL file written per row should be valid JSON") {
    val queue = createQueue()
    val writer = new JsonLineWriter(outputDir, queue, batchSize = 10, flushIntervalMs = 50)
    writer.start()

    queue.offer(sampleJobMetrics())
    Thread.sleep(300)
    writer.stop()

    val content = new String(Files.readAllBytes(Paths.get(outputDir, "job_metrics.jsonl")))
    val lines = content.trim.split("\n")
    assert(lines.length == 1)
    assert(lines.head.contains("\"metricType\":\"job\""))
    assert(lines.head.contains("\"appId\":\"test-app\""))
  }
}
