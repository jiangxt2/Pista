package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.doris.DorisJdbcSupport
import com.pista.spark.sql.doris.meta.DorisMetaManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import scala.util.Try

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

    // Phase 1: Equality Check (Overwrite on Write Conflict, Do Not Skip)
    if (!batchConfig.overwrite && context.metaManager.exists(_.isSucceeded(taskId))) {
      logInfo(s"[DorisConcurrentWriter] Task $taskId already succeeded, skipping")
      return BatchWriteResult(index = 0, rowCount = 0L, success = true)
    }

    // Record whether there is Meta history (for 2PC validation to distinguish the first run from expired transaction cleanup)
    val hasMetaHistory = context.metaManager.exists(_.hasAnyRecord(taskId))

    context.metaManager.foreach(_.upsertRunning(
      taskId,
      batchConfig.database,
      batchConfig.table,
      batchConfig.partitionDate,
      batchConfig.writeMode.value,
      batchConfig.sourceTable,
      batchConfig.resolvePartitionFilter()
    ))

    // Phase 2: Write Clean Environment Before Writing (2PC)
    if (batchConfig.enable2PC) {
      val coordinator = createTransactionCoordinator()
      val aborted = coordinator.abortByLabel(batchConfig)
      if (aborted > 0) logWarning(s"[DorisConcurrentWriter] Aborted $aborted stale pre-committed transactions")
    }

    // FE node is parsed by DorisFENodeResolver in buildBatchConfig (leader takes precedence), take the first one directly.
    val feNode = batchConfig.fenodes.split(",").head.trim
    logInfo(s"[DorisConcurrentWriter] Selected FE node: $feNode")

    // Phase 3a: Batch Overwrite Preparation (overwrite=true AND partitionDate IS NULL)
    val tableSwapMgrOpt = if (batchConfig.overwrite && !batchConfig.isPartitionOverwrite) {
      try {
        val mgr = createTableSwapManager(batchConfig, jobTimestamp)
        mgr.createTempTable()
        Some(mgr)
      } catch {
        case e: Exception =>
          context.metaManager.foreach(_.markFailure(taskId, e.getMessage))
          throw e
      }
    } else None

    // Phase 3b: Shard preparation for overwrite=true and partitionDate not null
    val partitionMgrOpt = if (batchConfig.isPartitionOverwrite) {
      try {
        Some(preparePartitionOverwrite(batchConfig, jobTimestamp))
      } catch {
        case e: Exception =>
          context.metaManager.foreach(_.markFailure(taskId, e.getMessage))
          throw e
      }
    } else None

    // Phase 4: Connector Write (Change the target table to a temporary table when whole-table overwrite write occurs)
    val writeConfig = tableSwapMgrOpt match {
      case Some(mgr) => batchConfig.copy(fenodes = feNode, table = mgr.tempTableName)
      case None      => batchConfig.copy(fenodes = feNode)
    }
    val connectorWriter = createConnectorWriter(writeConfig, sourceDf, jobTimestamp)

    val result = try {
      connectorWriter.write()
    } catch {
      case e: Exception => abortAndCleanup(e, "Write failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
    }

    // Phase 5a: Complete Table Overwrite Replacement (ALTER TABLE REPLACE)
    if (result.success) {
      try {
        tableSwapMgrOpt.foreach(_.replaceTable())
      } catch {
        case e: Exception => abortAndCleanup(e, "ALTER TABLE REPLACE failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
      }
    }

    // Phase 5b: PARTITION REPLACEMENT (REPLACE PARTITION)
    if (result.success) {
      try {
        partitionMgrOpt.foreach(_.replacePartitions())
      } catch {
        case e: Exception => abortAndCleanup(e, "REPLACE PARTITION failed", taskId, tableSwapMgrOpt, partitionMgrOpt)
      }
    }

    // Phase 6: 2PC strict validation (polling wait) VISIBLE)
    if (result.success && batchConfig.enable2PC) {
      try {
        val coordinator = createTransactionCoordinator()
        val isVisible = coordinator.validateAllVisible(batchConfig, maxWaitMs = 30000)
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
        logInfo("[DorisConcurrentWriter] 2PC transaction validation passed (all visible)")
      } catch {
        case e: Exception =>
          context.metaManager.foreach(_.markFailure(taskId, e.getMessage))
          throw e
      }
    }

    // Phase 7: Update Meta State
    val writtenRows = resolveWrittenRows(batchConfig, result.rowCount)
    if (result.success) {
      context.metaManager.foreach(_.markSuccess(taskId, writtenRows))
      logInfo(s"[DorisConcurrentWriter] Task $taskId succeeded, written rows: $writtenRows")
    } else {
      context.metaManager.foreach(_.markFailure(taskId, result.errorMessage))
      logError(s"[DorisConcurrentWriter] Task $taskId failed")
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
    context.metaManager.foreach(_.markFailure(taskId, e.getMessage))
    throw e
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
    logInfo(s"[DorisConcurrentWriter] PartitionOverwrite: target=${config.targetPartitions.mkString(",")}")
    partitionManager.prepareTempPartitions()
    partitionManager
  }

  private def resolveWrittenRows(config: DorisBatchConfig, rowCountFromWriter: Long): Long =
    if (rowCountFromWriter >= 0) rowCountFromWriter else queryDorisCount(config)

  protected def buildTaskId(config: DorisBatchConfig, clusterName: Option[String]): String = {
    val cluster = clusterName.getOrElse("default_cluster")
    val base = s"${cluster}_${config.labelPrefix}_${config.database}_${config.table}"

    config.resolvePartitionFilter() match {
      case Some(filter) =>
        val full = s"${base}_$filter"
        if (full.getBytes("UTF-8").length <= 2600) full
        else s"${base}_${sha256(filter)}"
      case None =>
        s"${base}_all"
    }
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
