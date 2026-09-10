package com.pista.spark.sql.execution.checkpoint

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.execution.datasources.reader.InputConfig
import com.pista.spark.sql.execution.datasources.writer.OutputConfig
import com.pista.spark.util.LogRedaction
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID
import scala.util.{Failure, Success, Try}

/**
 * Physical Manager (Singleton)
 *
 * Function:
 * 1. Manage the materialized catalog of DataFrames at the Job level
 * 2. Track the lifecycle of all materialized caches
 * 3. Provide manual release and automatic cleanup functionality.
 * 4. Support multiple protocols (HDFS, S3, local)
 *
 * Here, checkpoint means materializing a DataFrame as a managed Parquet or ORC file.
 * This manager handles the materialized lifecycle of DataFrames and does not undertake cross-file task scheduling or recovery.
 *
 * Usage examples: * Usage examples:
 * {{{
 *   // initialization
 *   MaterializationManager.initialize(spark, "hdfs://namenode:8020/checkpoints")
 *
 *   // Create a materialization.
 *   val handle = MaterializationManager.checkpoint(df, eager = true)
 *   val checkpointedDF = handle.dataFrame
 *
 *   // Manual release (optional)
 *   handle.release()
 *
 *   // Auto cleanup (at SparkSession shutdown)
 *   MaterializationManager.cleanup()
 * }}}
 *
 */
object MaterializationManager extends Logging {

  // ========== State Management ==========

  @volatile private var initialized = false
  @volatile private var baseDir: String = _
  @volatile private var jobDir: String = _
  @volatile private var jobId: String = _
  @volatile private var sparkSession: SparkSession = _

  private val registry = new CheckpointRegistry()
  private val writer = new CheckpointWriter()
  private val reader = new CheckpointReader()

  @volatile private var metadata: CheckpointMetadata = _
  @volatile private var shutdownHookAdded = false

  // ========== Initialize ==========

  /**
   * Initialize MaterializationManager
   *
   * @param spark SparkSession
   * @param basePath checkpoint Base path (supports protocols such as hdfs://, s3://, file://)
   */
  def initialize(spark: SparkSession, basePath: String): Unit = synchronized {
    if (initialized) {
      logWarning("MaterializationManager already initialized, skipping")
    } else {
      sparkSession = spark
      baseDir = basePath.stripSuffix("/")

      // Generate Job ID: applicationId + timestamp
      val appId = spark.sparkContext.applicationId
      val timestamp = System.currentTimeMillis()
      jobId = s"${appId}_$timestamp"

      // create the Job directory Job directory
      jobDir = s"$baseDir/$jobId"

      logInfo(s"Initializing MaterializationManager")
      logInfo(s"  Base directory: $baseDir")
      logInfo(s"  Job directory: $jobDir")
      logInfo(s"  Job ID: $jobId")
      logInfo(s"  Application ID: $appId")

      // create directory
      createDirectory(jobDir)

      // Initialize metadata
      metadata = CheckpointMetadataManager.create(jobId, appId)
      saveMetadata()

      // register Shutdown Hook
      registerShutdownHook()

      initialized = true
      logInfo("MaterializationManager initialized successfully")
    }
  }

  /**
   * Check if initialization is complete
   */
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      throw PistaErrors.checkpointNotInitializedError()
    }
  }

  // ========== Checkpoint operation ==========

  /**
   * create checkpoint
   *
   * The Parquet write is the sole Spark action; materialization completes during that write.
   * eager parameter retained to maintain API compatibility and no longer triggering additional count().
   *
   * @param df          The DataFrame to be checkpointed.
   * @param eager       retained deprecated parameter; no longer triggers an additional count Action
   * @param format      File format (parquet or orc)
   * @param compression Compression algorithm (default: zstd)
   * @return CheckpointHandle
   */
  def checkpoint(
    df: DataFrame,
    eager: Boolean = true,
    format: String = "parquet",
    compression: String = "zstd"
  ): CheckpointHandle = {
    ensureInitialized()

    val id = UUID.randomUUID()
    val checkpointPath = s"$jobDir/checkpoint-$id"

    logInfo(s"Creating checkpoint: $id")
    logInfo(s"  Path: $checkpointPath")

    // overwrite checkpoint
    val config = OutputConfig(
      path = Some(checkpointPath),
      format = format,
      mode = "Overwrite",
      partitionBy = Seq.empty,
      options = Map("compression" -> compression)
    )

    writer.write(df, config)

    // Use Reader to read DataFrame checkpointed later.
    val inputConfig = InputConfig(
      enabled = true,
      path = Some(checkpointPath),
      format = format,
      schema = None,
      predicates = Seq.empty,
      columns = Seq.empty,
      partitionFilters = Map.empty,
      options = Map.empty
    )

    val checkpointedDF = reader.read(sparkSession, inputConfig)

    logInfo(s"Checkpoint written to: $checkpointPath (eager=$eager)")

    // create a handle
    val handle = CheckpointHandle(
      id = id,
      dataFrame = checkpointedDF,
      path = checkpointPath,
      createdAt = System.currentTimeMillis()
    )

    // register
    registry.register(handle)

    // Update metadata
    synchronized {
      metadata = CheckpointMetadataManager.addCheckpoint(metadata, id, checkpointPath)
      saveMetadata()
    }

    logInfo(s"Checkpoint created successfully: $id")
    logInfo(s"Total active checkpoints: ${registry.size}")

    handle
  }

  /**
   * checkpoint the specified checkpoint
   *
   * @param handle CheckpointHandle
   */
  def release(handle: CheckpointHandle): Unit = {
    ensureInitialized()

    logInfo(s"Releasing checkpoint: ${handle.id}")

    // Remove from registry
    registry.unregister(handle.id) match {
      case Some(_) =>
        // delete directory
        deleteDirectory(handle.path)

        // Update metadata
        synchronized {
          metadata = CheckpointMetadataManager.markReleased(metadata, handle.id)
          saveMetadata()
        }

        logInfo(s"Checkpoint released: ${handle.id}")
        logInfo(s"Remaining active checkpoints: ${registry.size}")

      case None =>
        logWarning(s"Checkpoint not found in registry: ${handle.id}")
    }
  }

  /**
   * checkpoint all
   */
  def releaseAll(): Unit = {
    ensureInitialized()

    val handles = registry.getAll
    logInfo(s"Releasing all checkpoints: ${handles.size} total")

    handles.foreach { handle =>
      Try(release(handle)) match {
        case Success(_) =>
          logInfo(s"Released checkpoint: ${handle.id}")
        case Failure(e) =>
          logError(
            s"Failed to release checkpoint ${handle.id} with ${LogRedaction.exceptionName(e)}",
            LogRedaction.sanitizedThrowable(e))
      }
    }

    logInfo("All checkpoints released")
  }

  /**
   * Get all active checkpoints
   */
  def getActiveCheckpoints: Seq[CheckpointHandle] = {
    ensureInitialized()
    registry.getAll
  }

  /**
   * Get checkpoint count
   */
  def getCheckpointCount: Int = {
    if (!initialized) 0 else registry.size
  }

  // ========== Clean ==========

  /**
   * clean all resources
   *
   * Delete the entire Job directory (including all checkpoints and metadata).
   * job completes. Job automatically called at the end (via) Shutdown Hook).
   */
  def cleanup(): Unit = {
    if (!initialized) {
      logInfo("MaterializationManager not initialized, nothing to cleanup")
    } else {
      logInfo("Cleaning up MaterializationManager...")
      logInfo(s"  Job directory: $jobDir")
      logInfo(s"  Active checkpoints: ${registry.size}")

      // clear registry registration
      registry.clear()

      // delete entire Job directory
      deleteDirectory(jobDir)

      // Reset state
      initialized = false
      baseDir = null
      jobDir = null
      jobId = null
      metadata = null
      sparkSession = null

      logInfo("MaterializationManager cleanup completed")
    }
  }

  // ========== File System Operations ==========

  /**
   * create directory
   */
  private def createDirectory(path: String): Unit = {
    val hadoopPath = new Path(path)
    val fs = FileSystem.get(hadoopPath.toUri, sparkSession.sparkContext.hadoopConfiguration)

    if (!fs.exists(hadoopPath)) {
      fs.mkdirs(hadoopPath)
      logInfo(s"Created directory ${LogRedaction.fingerprint(path.toString)}")
    } else {
      logInfo(s"Directory already exists: ${LogRedaction.fingerprint(path.toString)}")
    }
  }

  /**
   * delete directory
   */
  private def deleteDirectory(path: String): Unit = {
    Try {
      val hadoopPath = new Path(path)
      val fs = FileSystem.get(hadoopPath.toUri, sparkSession.sparkContext.hadoopConfiguration)

      if (fs.exists(hadoopPath)) {
        fs.delete(hadoopPath, true)
        logInfo(s"Deleted directory ${LogRedaction.fingerprint(path.toString)}")
      } else {
        logInfo(s"Directory does not exist: ${LogRedaction.fingerprint(path.toString)}")
      }
    } match {
      case Success(_) =>
        logInfo(s"Successfully deleted: ${LogRedaction.fingerprint(path.toString)}")
      case Failure(e) =>
        logError(
          s"Directory deletion failed with ${LogRedaction.exceptionName(e)}",
          LogRedaction.sanitizedThrowable(e))
    }
  }

  /**
   * Persist metadata
   */
  private def saveMetadata(): Unit = {
    Try {
      CheckpointMetadataManager.save(metadata, jobDir, sparkSession)
    } match {
      case Success(_) =>
        logDebug("Metadata saved successfully")
      case Failure(e) =>
        logError(
          s"Failed to save metadata with ${LogRedaction.exceptionName(e)}",
          LogRedaction.sanitizedThrowable(e))
    }
  }

  // ========== Shutdown Hook ==========

  /**
   * register Shutdown Hook
   */
  private def registerShutdownHook(): Unit = synchronized {
    if (!shutdownHookAdded) {
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        logInfo("Shutdown hook triggered, cleaning up checkpoints...")
        Try(cleanup()) match {
          case Success(_) =>
            logInfo("Shutdown cleanup completed")
          case Failure(e) =>
            logError(
              s"Shutdown cleanup failed with ${LogRedaction.exceptionName(e)}",
              LogRedaction.sanitizedThrowable(e))
        }
      }))
      shutdownHookAdded = true
      logInfo("Shutdown hook registered")
    }
  }

  // ========== Debug Information ==========

  /**
   * Print current state
   */
  def printStatus(): Unit = {
    if (!initialized) {
      logInfo("MaterializationManager is not initialized")
    } else {
      logInfo(s"MaterializationManager has ${registry.size} active checkpoint(s)")

      if (registry.size > 0) {
        registry.getAll.sortBy(_.createdAt).foreach { handle =>
          logInfo(s"Checkpoint ${handle.id}: age=${handle.ageSeconds}s")
        }
      }
    }
  }
}
