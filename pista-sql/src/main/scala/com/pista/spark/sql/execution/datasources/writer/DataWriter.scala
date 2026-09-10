package com.pista.spark.sql.execution.datasources.writer

import com.pista.spark.errors.PistaErrors
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * DataWriter trait - open for extension via AbstractDataWriter
 *
 */
trait DataWriter {
  def name: String
  def write(df: DataFrame, config: OutputConfig): Unit
  def validateConfig(config: OutputConfig): ValidationResult
}

/**
 * Abstract base class - direct subclass of DataWriter
 * Provides common functionality for all writers
 *
 */
abstract class AbstractDataWriter extends DataWriter with Logging {

  // ========== Configurable Parameters (Subclasses Can Override) ==========

  /** Engine Name (For Logging) */
  protected def engineName: String = name

  // ========== Main Workflow ==========

  override def write(df: DataFrame, config: OutputConfig): Unit = {
    // 1. Validate configuration
    val validation = validateConfig(config)
    if (!validation.valid)
      throw PistaErrors.writerValidationError(
        validation.errorMessage.getOrElse("Invalid configuration")
      )

    // 2. Streaming mode: skip cache + count and write directly
    if (config.streamingMode) {
      logInfo(s"[$engineName] Streaming mode enabled, skipping cache and stats collection")
      writeInternal(df, DataStats.empty, config)
    } else {
      // 3. Batch Processing Mode: Ensure DataFrame is persisted (indicating whether records are newly added by this write)
      val (cachedDf, cacheAddedByWriter) = ensureCached(df)

      // 4. Collect statistical information and write them out, and release the allocated memory after completion.
      try {
        val stats = collectDataStats(cachedDf)
        logDataStats(stats)
        writeInternal(cachedDf, stats, config)
      } finally {
        if (cacheAddedByWriter) cachedDf.unpersist(blocking = false)
      }
    }
  }

  /**
   * Internal write method - subclass must implement
   */
  protected def writeInternal(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig
  ): Unit

  // ========== Cache Management ==========

  /**
   * Ensure DataFrame is cached*
   * @return (cachedDf, cacheAddedByWriter) — Latter is true indicating the cache was added by this invocation, the caller is responsible for unpersisting
   */
  private def ensureCached(df: DataFrame): (DataFrame, Boolean) = {
    if (!df.storageLevel.useMemory && !df.storageLevel.useDisk) {
      logInfo(s"[$engineName] DataFrame not cached, caching with MEMORY_AND_DISK...")
      val persisted = df.persist(StorageLevel.MEMORY_AND_DISK)
      persisted.rdd.setName(s"pista_writer[$engineName]")
      (persisted, true)
    } else {
      logInfo(s"[$engineName] DataFrame already cached: ${df.storageLevel}")
      (df, false)
    }
  }

  /**
   * Collect statistical data from Spark cached information*
   */
  private def collectDataStats(df: DataFrame): DataStats = {
    val sparkContext = df.sparkSession.sparkContext
    val rddId = df.rdd.id

    // Attempt to retrieve initial checkpoint information
    val initialCacheInfo = sparkContext.getRDDStorageInfo.find(_.id == rddId)
    logCacheInfo(initialCacheInfo)

    // Trigger a count() and obtain the latest checkpoint information
    val rowCount = df.count()
    val finalCacheInfo = sparkContext.getRDDStorageInfo.find(_.id == rddId)

    // Build statistics metrics
    buildDataStats(rowCount, finalCacheInfo, df.rdd.getNumPartitions)
  }

  /**
   * Log information about the record cache
   */
  private def logCacheInfo(cacheInfo: Option[org.apache.spark.storage.RDDInfo]): Unit =
    cacheInfo match {
      case Some(info) if info.isCached =>
        logInfo(s"[$engineName] Cache found - partitions: ${info.numCachedPartitions}, " +
          s"memory: ${info.memSize} bytes, disk: ${info.diskSize} bytes")
      case _ =>
        logInfo(s"[$engineName] No cache info, materializing...")
    }

  /**
   * Build DataStats based on cached information.
   */
  private def buildDataStats(
    rowCount: Long,
    cacheInfo: Option[org.apache.spark.storage.RDDInfo],
    defaultPartitions: Int
  ): DataStats =
    cacheInfo match {
      case Some(info) =>
        DataStats(
          rowCount = Some(rowCount),
          memorySize = Some(info.memSize),
          diskSize = Some(info.diskSize),
          numPartitions = info.numCachedPartitions,
          isCached = true
        )
      case None =>
        DataStats(
          rowCount = Some(rowCount),
          memorySize = None,
          diskSize = None,
          numPartitions = defaultPartitions,
          isCached = false
        )
    }

  /**
   * Batch size for JDBC recommendation
   */
  protected def recommendedBatchSize(stats: DataStats): Int =
    stats.rowCount match {
      case Some(rows) if rows < 10000 => 1000
      case Some(rows) if rows < 100000 => 5000
      case Some(rows) if rows < 1000000 => 10000
      case _ => 20000
    }

  /**
   * Record statistics in the log.
   */
  private def logDataStats(stats: DataStats): Unit = {
    val rowInfo = stats.rowCount.map(r => s"$r rows").getOrElse("unknown rows")
    val memInfo = stats.memorySize.map(s => f"${s / 1024.0 / 1024}%.2f MB memory").getOrElse("")
    val diskInfo = stats.diskSize.map(s => f"${s / 1024.0 / 1024}%.2f MB disk").getOrElse("")
    val sizeInfo = Seq(memInfo, diskInfo).filter(_.nonEmpty).mkString(", ")

    logInfo(s"[$engineName] Data stats: $rowInfo" +
      (if (sizeInfo.nonEmpty) s", $sizeInfo" else "") +
      s", ${stats.numPartitions} partitions")
  }
}

/**
 * DataFrame Statistics (\(*) Read from Cache (\*))
 *
 * @param rowCount Total number of rows
 * @param memorySize Memory size (bytes)
 * @param diskSize Disk size (bytes)
 * @param numPartitions Number of partitions
 * @param isCached Whether cached
 */
case class DataStats(
  rowCount: Option[Long],
  memorySize: Option[Long],
  diskSize: Option[Long],
  numPartitions: Int,
  isCached: Boolean
)

object DataStats {
  /**
   * Empty statistics (for streaming mode)
   */
  val empty: DataStats = DataStats(
    rowCount = None,
    memorySize = None,
    diskSize = None,
    numPartitions = 0,
    isCached = false
  )
}

/**
 * Output configuration
 *
 * @param streamingMode Whether streaming mode (skip cache + count in streaming mode)
 */
case class OutputConfig(
  path: Option[String],
  format: String,
  mode: String,
  partitionBy: Seq[String],
  options: Map[String, String],
  streamingMode: Boolean = false
)

/**
 * Configuration validation result
 */
case class ValidationResult(valid: Boolean, errorMessage: Option[String])
