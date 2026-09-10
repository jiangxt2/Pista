package com.pista.spark.sql.metrics.writer

import com.pista.spark.sql.metrics.model.Metrics
import org.apache.spark.internal.Logging

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
 * JSON Lines writer
 *
 * Write metrics to JSON Lines file asynchronously by a background Daemon thread.
 * Batch write and scheduled refresh are supported.
 *
 * @param outputDir       outputDir
 * @param metrics_queue  metrics queue
 * @param batchSize      Batch write size
 * @param flushIntervalMs Interval for flushing updates (milliseconds)
 *
 */
class JsonLineWriter(
  outputDir: String,
  queue: MetricsQueue[Metrics],
  batchSize: Int,
  flushIntervalMs: Long
) extends Logging {

  private val running = new AtomicBoolean(false)
  private val writeCount = new AtomicLong(0)
  private val errorCount = new AtomicLong(0)
  private var writerThread: Thread = _

  private val outputPath = Paths.get(outputDir)

  /**
   * Start writer
   */
  def start(): Unit = {
    if (running.compareAndSet(false, true)) {
      ensureOutputDir()
      writerThread = new Thread(new WriterRunnable, "metrics-json-writer")
      writerThread.setDaemon(true)
      writerThread.start()
      logInfo(s"JsonLineWriter started, output dir: $outputDir")
    }
  }

  /**
   * stop writer
   */
  def stop(): Unit = {
    if (running.compareAndSet(true, false)) {
      writerThread.interrupt()
      // Last refresh checkpoint
      flushAll()
      logInfo(s"JsonLineWriter stopped, total writes: ${writeCount.get()}, errors: ${errorCount.get()}")
    }
  }

  /**
   * Get write statistics
   */
  def stats: WriterStats = WriterStats(
    writeCount = writeCount.get(),
    errorCount = errorCount.get(),
    queueSize = queue.size,
    droppedCount = queue.droppedCount
  )

  private def ensureOutputDir(): Unit = {
    if (!Files.exists(outputPath)) {
      Files.createDirectories(outputPath)
      logInfo(s"Created output directory: $outputDir")
    }
  }

  private def flushAll(): Unit = {
    while (!queue.isEmpty) {
      val batch = queue.drain(batchSize)
      if (batch.nonEmpty) writeBatch(batch)
    }
  }

  private def writeBatch(batch: Seq[Metrics]): Unit = {
    // Write into different files based on metric type groups
    batch.groupBy(_.metricType).foreach { case (metricType, metrics) =>
      val filePath = outputPath.resolve(s"${metricType}_metrics.jsonl")
      try {
        val writer = new BufferedWriter(new FileWriter(filePath.toFile, true))
        try {
          metrics.foreach { m =>
            writer.write(Metrics.toJson(m))
            writer.newLine()
          }
          writer.flush()
          writeCount.addAndGet(metrics.size)
        } finally {
          writer.close()
        }
      } catch {
        case e: Exception =>
          errorCount.addAndGet(metrics.size)
          logWarning(s"Metrics write failed with ${e.getClass.getSimpleName}")
      }
    }
  }

  private class WriterRunnable extends Runnable {
    override def run(): Unit = {
      while (running.get()) {
        try {
          val batch = queue.drain(batchSize)
          if (batch.nonEmpty) {
            writeBatch(batch)
          } else {
            Thread.sleep(flushIntervalMs)
          }
        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
          case e: Exception =>
            logWarning(s"Metrics writer thread failed with ${e.getClass.getSimpleName}")
        }
      }
    }
  }
}

/**
 * Writer statistics
 */
case class WriterStats(
  writeCount: Long,
  errorCount: Long,
  queueSize: Int,
  droppedCount: Long
)
