package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.doris.DorisJdbcSupport
import com.pista.spark.sql.doris.meta.DorisMetaManager
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.util.Try
import scala.util.control.NonFatal

/**
 * Doris batch orchestrator
 *
 * Core Design: Task-Level Idempotence Check + Full-Node Bulk Write from Single FE + Standby Switch
 *
 * Unlike the ClickHouse path, Doris coordinates tasks directly without ShardOrchestrator.
 * - ClickHouse: By primary key hash partitioning, each node writes a portion of the data.
 * - Doris: No sharding, a single FE node writes full dataset, and the Connector internally distributes it via Stream Load to BE
 *
 * @param batchConfig  Batch write configuration (Fenodes have been parsed by DorisFENodeResolver, leader takes precedence)
 * @param context      Concurrent context (clusterName + metaManager)
 * @param sourceDf     Source DataFrame (full dataset)
 * @param jobTimestamp Job Timestamp (used for naming temp partitions)
 */
class DorisConcurrentWriter(
  batchConfig:  DorisBatchConfig,
  context:      DorisConcurrentContext,
  sourceDf:     DataFrame,
  jobTimestamp: Long
) extends DorisJdbcSupport with Logging {

  def write(): BatchWriteResult = {
    val taskId = buildTaskId(batchConfig, context.clusterName)
    val taskRef = LogRedaction.fingerprint(taskId)
    val started = System.nanoTime()
    val mode = if (batchConfig.isPartitionOverwrite) "partition-overwrite"
      else if (batchConfig.overwrite) "table-overwrite" else "append"
    logInfo(s"[DorisConcurrentWriter] taskRef=$taskRef attempt=$jobTimestamp " +
      s"targetRef=${LogRedaction.fingerprint(s"${batchConfig.database}.${batchConfig.table}")} " +
      s"mode=$mode metadataEnabled=${context.metaManager.isDefined} " +
      s"partitionCount=${batchConfig.targetPartitions.size} " +
      s"taskIdChars=${DorisMetaManager.characterCount(taskId)} " +
      s"rdateChars=${DorisMetaManager.characterCount(batchConfig.partitionDate)}")

    // Phase 1: Equality Check (Overwrite on Write Conflict, Do Not Skip)
    if (!batchConfig.overwrite && inStage("metadata.check-success", taskId) {
      context.metaManager.exists(_.isSucceeded(taskId))
    }) {
      logInfo(s"[DorisConcurrentWriter] taskRef=$taskRef outcome=skipped-already-succeeded")
      return BatchWriteResult(index = 0, rowCount = 0L, success = true)
    }

    inStage("metadata.running", taskId) { context.metaManager.foreach(_.upsertRunning(
      taskId,
      batchConfig.database,
      batchConfig.table,
      batchConfig.partitionDate,
      batchConfig.writeMode.value,
      batchConfig.sourceTable,
      batchConfig.resolvePartitionFilter()
    )) }

    // Phase 2: Write Clean Environment Before Writing (2PC)
    if (batchConfig.enable2PC) {
      val coordinator = createTransactionCoordinator()
      val aborted = inStage("transaction.prepare", taskId) { coordinator.abortByLabel(batchConfig) }
      if (aborted > 0) logWarning(s"[DorisConcurrentWriter] Aborted $aborted stale pre-committed transactions")
    }

    // FE node is parsed by DorisFENodeResolver in buildBatchConfig (leader takes precedence), take the first one directly.
    val feNode = batchConfig.fenodes.split(",").head.trim
    logDebug(s"[DorisConcurrentWriter] taskRef=$taskRef feRef=${LogRedaction.fingerprint(feNode)}")

    // Phase 3a: Batch Overwrite Preparation (overwrite=true AND partitionDate IS NULL)
    val tableSwapMgrOpt = if (batchConfig.overwrite && !batchConfig.isPartitionOverwrite) {
      try {
        val mgr = createTableSwapManager(batchConfig, jobTimestamp)
        inStage("prepare.table", taskId) { mgr.createTempTable() }
        Some(mgr)
      } catch {
        case e: Exception =>
          markFailurePreserving(taskId, e)
          throw e
      }
    } else None

    // Phase 3b: Shard preparation for overwrite=true and partitionDate not null
    val partitionMgrOpt = if (batchConfig.isPartitionOverwrite) {
      try {
        Some(inStage("prepare.partitions", taskId) { preparePartitionOverwrite(batchConfig, jobTimestamp) })
      } catch {
        case e: Exception =>
          markFailurePreserving(taskId, e)
          throw e
      }
    } else None

    // Phase 4: Connector Write (Change the target table to a temporary table when whole-table overwrite write occurs)
    val writeConfig = tableSwapMgrOpt match {
      case Some(mgr) => batchConfig.copy(fenodes = feNode, table = mgr.tempTableName)
      case None      => batchConfig.copy(fenodes = feNode)
    }
    val result = try {
      inStage("write", taskId) { createConnectorWriter(writeConfig, sourceDf, jobTimestamp).write() }
    } catch {
      case e: Exception => abortAndCleanup(e, "Write failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
    }

    // Phase 5a: Complete Table Overwrite Replacement (ALTER TABLE REPLACE)
    if (result.success) {
      try {
        inStage("publish.table", taskId) { tableSwapMgrOpt.foreach(_.replaceTable()) }
      } catch {
        case e: Exception => abortAndCleanup(e, "ALTER TABLE REPLACE failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
      }
    }

    // Phase 5b: PARTITION REPLACEMENT (REPLACE PARTITION)
    if (result.success) {
      try {
        inStage("publish.partitions", taskId) { partitionMgrOpt.foreach(_.replacePartitions()) }
      } catch {
        case e: Exception => abortAndCleanup(e, "REPLACE PARTITION failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
      }
    }

    // Phase 6: 2PC strict validation (polling wait) VISIBLE)
    if (result.success && batchConfig.enable2PC) {
      try {
        val coordinator = createTransactionCoordinator()
        val isVisible = inStage("verify.transactions", taskId) {
          coordinator.validateAllVisible(batchConfig, maxWaitMs = 30000)
        }
        if (!isVisible) {
          val statuses = coordinator.queryTransactionStatuses(batchConfig)
          val hasPrecommitted = statuses.exists(s => s == "PRECOMMITTED" || s == "PREPARE")
          if (hasPrecommitted) {
            val aborted = Try(coordinator.abortByLabel(batchConfig)).getOrElse(0)
            logWarning(s"[DorisConcurrentWriter] Timeout with PRECOMMITTED transactions, aborted $aborted")
          } else if (statuses.nonEmpty) {
            logWarning(s"[DorisConcurrentWriter] Timeout but all transactions COMMITTED, data likely visible soon. " +
              s"statuses=${statuses.mkString(",")}")
          }
          throw PistaErrors.dorisWriterError(
            s"2PC transaction validation failed: not all transactions visible within timeout")
        }
        logDebug(s"[DorisConcurrentWriter] taskRef=$taskRef transactionsVisible=true")
      } catch {
        case e: Exception =>
          markFailurePreserving(taskId, e)
          throw e
      }
    }

    // Phase 7: Update Meta State
    val writtenRows = resolveWrittenRows(batchConfig, result.rowCount)
    if (result.success) {
      inStage("metadata.success", taskId) { context.metaManager.foreach(_.markSuccess(taskId, writtenRows)) }
      logInfo(s"[DorisConcurrentWriter] taskRef=$taskRef outcome=succeeded writtenRows=$writtenRows " +
        s"elapsedMs=${(System.nanoTime() - started) / 1000000}")
    } else {
      inStage("metadata.failure", taskId) { context.metaManager.foreach(_.markFailure(taskId, result.errorMessage)) }
      logError(s"[DorisConcurrentWriter] taskRef=$taskRef outcome=failed")
    }

    result.copy(rowCount = writtenRows)
  }

  protected def createConnectorWriter(
    config:       DorisBatchConfig,
    sourceDf:     DataFrame,
    jobTimestamp: Long
  ): SparkDorisConnectorWriter =
    new SparkDorisConnectorWriter(config, sourceDf, jobTimestamp)

  protected def createTransactionCoordinator(): DorisTransactionCoordinator =
    new DorisTransactionCoordinator()

  protected def createTableSwapManager(
    config:       DorisBatchConfig,
    jobTimestamp: Long
  ): DorisTableSwapManager =
    new DorisTableSwapManager(config, jobTimestamp)

  protected def createPartitionManager(
    config:       DorisBatchConfig,
    jobTimestamp: Long
  ): DorisPartitionManager =
    new DorisPartitionManager(config, jobTimestamp)

  private def abortAndCleanup(
    e:                Exception,
    msg:              String,
    taskId:           String,
    tableSwapMgrOpt:  Option[DorisTableSwapManager],
    partitionMgrOpt:  Option[DorisPartitionManager]
  ): Nothing = {
    if (batchConfig.enable2PC) Try {
      val aborted = createTransactionCoordinator().abortByLabel(batchConfig)
      logWarning(s"[DorisConcurrentWriter] $msg, aborted $aborted pre-committed transactions")
    }
    tableSwapMgrOpt.foreach(m => Try(m.cleanupTempTable()))
    partitionMgrOpt.foreach(m => Try(m.cleanupTempPartitions()))
    markFailurePreserving(taskId, e)
    throw e
  }

  private def markFailurePreserving(taskId: String, original: Throwable): Unit =
    try context.metaManager.foreach(_.markFailure(taskId, LogRedaction.exceptionName(original)))
    catch {
      case NonFatal(updateError) =>
        if (updateError ne original) original.addSuppressed(updateError)
    }

  private def inStage[T](stage: String, taskId: String)(operation: => T): T = {
    val taskRef = LogRedaction.fingerprint(taskId)
    val started = System.nanoTime()
    logDebug(s"[DorisConcurrentWriter] taskRef=$taskRef stage=$stage outcome=started")
    try {
      val result = operation
      logDebug(s"[DorisConcurrentWriter] taskRef=$taskRef stage=$stage outcome=completed " +
        s"elapsedMs=${(System.nanoTime() - started) / 1000000}")
      result
    } catch {
      case NonFatal(error) =>
        val message = s"[DorisConcurrentWriter] taskRef=$taskRef stage=$stage outcome=failed " +
          s"error=${LogRedaction.exceptionName(error)} elapsedMs=${(System.nanoTime() - started) / 1000000}"
        // Metadata logs own the SQLSTATE and schema diagnosis; avoid a duplicate ERROR.
        if (stage.startsWith("metadata.")) logDebug(message)
        else logError(message, LogRedaction.sanitizedThrowable(error))
        throw error
    }
  }

  private def preparePartitionOverwrite(
    config:       DorisBatchConfig,
    jobTimestamp: Long
  ): DorisPartitionManager = {
    if (config.targetPartitions.isEmpty)
      throw PistaErrors.dorisWriterError(
        "overwrite=true but partitionDate is empty. " +
        "Please set spark.pista.doris.partition.dateValue")

    val partitionManager = createPartitionManager(config, jobTimestamp)
    try partitionManager.prepareTempPartitions()
    catch {
      case NonFatal(error) =>
        Try(partitionManager.cleanupTempPartitions()).failed.foreach { cleanupError =>
          if (cleanupError ne error) error.addSuppressed(cleanupError)
        }
        throw error
    }
    partitionManager
  }

  private def resolveWrittenRows(config: DorisBatchConfig, rowCountFromWriter: Long): Long =
    if (rowCountFromWriter >= 0) rowCountFromWriter else queryDorisCount(config)

  protected def buildTaskId(config: DorisBatchConfig, clusterName: Option[String]): String = {
    val cluster = clusterName.getOrElse("default_cluster")
    val base = s"${cluster}_${config.labelPrefix}_${config.database}_${config.table}"

    val filter = config.resolvePartitionFilter()
    val full = s"${base}_${filter.getOrElse("all")}"
    // Retain the old hash representation when it already fits the database key.
    val legacy = filter match {
      case Some(value) if full.getBytes("UTF-8").length > 2600 => s"${base}_${sha256(value)}"
      case _ => full
    }
    if (DorisMetaManager.characterCount(legacy) <= DorisMetaManager.TaskIdMaxChars) legacy
    else s"pista_sha256_${sha256(full)}"
  }

  private def sha256(s: String): String = {
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    digest.map("%02x".format(_)).mkString
  }
}

/**
 * Doris concurrent context
 *
 * @param clusterName  Logical cluster name (used for taskId generation)
 * @param metaManager optional metadata manager used for idempotency checks
 */
case class DorisConcurrentContext(
  clusterName:  Option[String],
  metaManager:  Option[DorisMetaManager]
)
