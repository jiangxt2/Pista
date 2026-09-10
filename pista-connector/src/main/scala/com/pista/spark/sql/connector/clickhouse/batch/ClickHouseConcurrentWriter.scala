package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.clickhouse.meta.record.{DataInfoRecord, DataRecord, RecordStatus}
import com.pista.spark.sql.clickhouse.meta.{MetaMachineManager, MetaManager}
import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.clickhouse.{ClickHouseBatchJdbcSupport, ClickHouseDialect, ClickHouseUDFs}
import com.pista.spark.sql.execution.checkpoint.{CheckpointHandle, MaterializationManager}
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.net.{URI, URISyntaxException}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
 * ClickHouse Distributed Shard Writer Utility Class
 *
 * Package concurrent orchestration logic for reuse by ClickHouseWriter.
 * Core Capability:
 * - Shards by primary-key hash and writes concurrently; primaryKey=None falls back to one direct shard.
 * - retry shards that fail automatically (up to context.maxRetries times)
 * - Meta Configuration: Auto-scaling, Rescaling, Standby Switch, Data Volume Verification
 *
 * parameter hierarchy corresponds to Spark write pipeline design:
 * - batchConfig(ClickHouseBatchConfig) corresponds to WriteJobDescriptionbatchConfig is a static configuration with no service dependencies.
 * - context (ClickHouseConcurrentContext) corresponds to TaskAttemptContext: Meta service + concurrent scheduling parameters
 *
 * @param sourceDf    Source DataFrame (will be cached and sharded by the primary key)
 * @param primaryKey Primary column name (optional) used for connector compatibility with hash % machineCount shardings
 * @param batchConfig Write configuration (target database table, JDBC options, batch write parameters, overwrite strategy, etc.)
 * @param context     Concurrent context (Meta service, cluster name, number of shards, threads, retry count)
 *
 */
class ClickHouseConcurrentWriter(
  sourceDf:    DataFrame,
  primaryKey:  Option[String],
  batchConfig: ClickHouseBatchConfig,
  context:     ClickHouseConcurrentContext
) extends ClickHouseBatchJdbcSupport with Logging {

  if (context.machineCount <= 0)
    throw new IllegalArgumentException(s"machineCount must be > 0, got ${context.machineCount}")

  if (context.machineCount > 1 && primaryKey.isEmpty)
    throw new IllegalArgumentException(
      s"primaryKey is required when machineCount > 1 (got machineCount=${context.machineCount}), " +
      "otherwise all shards write full data causing duplication")

  protected implicit val spark: SparkSession = sourceDf.sparkSession

  // Persist validated DataInfoRecord (containing correct id) for final state update
  private var verifiedDataInfoRecord: Option[DataInfoRecord] = None

  /**
   * Perform parallel shard write overwrite
   *
   * According to context.metaManager Existence determination leads to different initialization paths:
   * - Have meta: Meta Scheduler (machine allocation, resume from breakpoint, standby machine switch)
   * - No meta: Directly connect to hostAddress, single shard direct write
   *
   * @return ((shard write results, actual machine count))
   */
  def write(): (Seq[BatchWriteResult], Int) = {
    JdbcDialects.registerDialect(ClickHouseDialect)
    ClickHouseUDFs.registerAll(spark)

    val (_, machineAssignments, actualMachineCount, effectiveBatchConfig,
         onShardStartFn, onShardCompleteFn, skippedShards, getSkippedRecordFn) =
      context.metaManager match {
        case Some(mgr) => initWithMeta(mgr)
        case None      => initWithoutMeta()
      }

    val (dfToProcess, checkpointHandleOpt) = prepartitionIfConfigured(sourceDf)

    // Checkpoint files themselves serve as persistent data sources. Directly read the files without persisting.
    // Use DISK_ONLY persistence for multi-shard reuse when no checkpoint is configured.
    val (workingDf, didPersist) = checkpointHandleOpt match {
      case Some(_) =>
        logInfo("[ClickHouseConcurrentWriter] Checkpoint active: using checkpoint files as data source, skipping persist()")
        (dfToProcess, false)
      case None =>
        val cached = dfToProcess.persist(StorageLevel.DISK_ONLY)
        cached.rdd.setName(s"clickhouse_concurrent_source[${batchConfig.database}.${batchConfig.table}]")
        (cached, true)
    }
    try {
      val orchestrator = new ShardOrchestrator(
        machineCount    = actualMachineCount,
        maxThreads      = context.maxThreads,
        maxRetries      = context.maxRetries,
        logPrefix       = "[ClickHouseConcurrentWriter]",
        executeShard    = { (index, verbose) =>
          if (skippedShards.contains(index)) {
            logInfo(s"[ClickHouseConcurrentWriter] Shard $index skipped (already succeeded)")
            val existingRecord = getSkippedRecordFn(index)
            val rowCount    = existingRecord.map(_.insertDataVolume).getOrElse(0L)
            val sourceCount = existingRecord.map(_.originalDataVolume).getOrElse(0L)
            BatchWriteResult(index, rowCount, success = true,
              hostAddress = machineAssignments.getOrElse(index, ""), sourceRowCount = sourceCount)
          } else {
            val hostAddress = machineAssignments.getOrElse(index, "")
            val shardConfig = if (hostAddress.nonEmpty)
              effectiveBatchConfig.copy(jdbcUrl = ClickHouseConcurrentWriter.replaceHost(effectiveBatchConfig.jdbcUrl, hostAddress))
            else effectiveBatchConfig
            logInfo(s"[ClickHouseConcurrentWriter] Shard $index destination configured")
            val writer = KeyBasedBatchWriter(shardConfig, index, actualMachineCount, workingDf, primaryKey, verbose)
            Try(writer.write()).recover {
              case e: Exception =>
                logError(
                  s"[ClickHouseConcurrentWriter] Shard $index write failed with " +
                    LogRedaction.exceptionName(e),
                  LogRedaction.sanitizedThrowable(e))
                BatchWriteResult(
                  index,
                  0L,
                  success = false,
                  hostAddress = hostAddress,
                  errorMessage = LogRedaction.exceptionName(e))
            }.get
          }
        },
        onShardStart    = onShardStartFn,
        onShardComplete = onShardCompleteFn
      )
      (orchestrator.run(), actualMachineCount)
    } finally {
      if (didPersist) workingDf.unpersist(blocking = false)
      checkpointHandleOpt.foreach(h => scala.util.Try(MaterializationManager.release(h)))
    }
  }

  // ==================== With Meta Initialization Path ====================

  private type InitResult = (
    Option[MetaMachineManager],       // machineManagerOpt
    TrieMap[Int, String],             // machineAssignments
    Int,                              // actualMachineCount
    ClickHouseBatchConfig,            // effectiveBatchConfig
    Int => Unit,                      // onShardStart
    (Int, BatchWriteResult) => Unit,  // onShardComplete
    java.util.Set[Int],               // skippedShards
    Int => Option[DataRecord]         // getSkippedShardRecord
  )

  private def initWithMeta(mgr: MetaManager): InitResult = {
    val clusterName = context.clusterName.getOrElse(
      throw PistaErrors.clickHouseWriterError("clusterName is required when meta is enabled"))
    val clusterId = mgr.getClusterIdByName(clusterName)

    val machineMgr = new MetaMachineManager(mgr, clusterId)
    val shardCount = machineMgr.init(context.machineCount)

    if (shardCount == 0)
      throw PistaErrors.clickHouseWriterError(
        "No available ClickHouse machines. Check clickhouse_machine_info table or " +
        "clear failed records in clickhouse_data_records table (status=INITIAL/FAILURE).")

    val dataInfoRecord = buildDataInfoRecord(shardCount, clusterId)
    verifiedDataInfoRecord = mgr.verifyDataInfoRecord(dataInfoRecord, batchConfig.overwrite)

    verifiedDataInfoRecord.foreach { record =>
      mgr.updateTaskStatus(record.copy(status = RecordStatus.RUNNING_VALUE))
      logInfo(s"[ClickHouseConcurrentWriter] DataInfoRecord status set to RUNNING (indexSize=${record.indexSize})")
    }

    if (batchConfig.overwrite) {
      logInfo(s"[ClickHouseConcurrentWriter] Overwrite mode (${batchConfig.overwriteMode}): " +
        s"deleting existing data from ${batchConfig.database}.${batchConfig.table}")
      deleteClickHouseData(mgr, clusterName, clusterId, batchConfig.overwriteMode.toLowerCase)
    } else {
      val usedMachines = machineMgr.getAlreadyUsedMachines(dataInfoRecord).toSet
      if (usedMachines.nonEmpty) {
        machineMgr.removeFromPool(usedMachines)
        logInfo(s"[ClickHouseConcurrentWriter] Non-overwrite mode: removed ${usedMachines.size} used machines from pool")
      }
    }

    // Global delete success marks skipLocalDelete. Safe to retry skipping shard-level delete during retries (all node data cleared)
    val effectiveBatchConfig = if (batchConfig.overwrite)
      batchConfig.copy(skipLocalDelete = true)
    else
      batchConfig

    val existingRecords = (0 until shardCount).flatMap { index =>
      mgr.searchRecord(buildDataRecordTemplate(index, shardCount, clusterId))
    }.map(r => r.dataIndex -> r.hostAddress).toMap

    val assignments = TrieMap((0 until shardCount).map { index =>
      existingRecords.get(index) match {
        case Some(host) if host.nonEmpty =>
          index -> host
        case _ =>
          try index -> machineMgr.chooseOneMachine()
          catch {
            case _: Exception =>
              throw PistaErrors.clickHouseWriterError(
                s"Not enough available machines for new shards. " +
                s"Requested ${shardCount - existingRecords.size} new machines but pool exhausted.")
          }
      }
    }: _*)

    logInfo(s"[ClickHouseConcurrentWriter] Assigning $shardCount shards (requested: ${context.machineCount})")
    logInfo(s"[ClickHouseConcurrentWriter] Existing shards reused: ${existingRecords.size}, new: ${shardCount - existingRecords.size}")

    val verifiedRecords    = new ConcurrentHashMap[Int, DataRecord]()
    val skippedShards      = ConcurrentHashMap.newKeySet[Int]()
    val backupSwitchCounts = TrieMap.empty[Int, Int]
    val backupSwitchHosts  = TrieMap.empty[Int, scala.collection.mutable.ArrayBuffer[String]]

    def onShardStart(index: Int): Unit = {
      val hostAddress = assignments.getOrElse(index, "")
      val template = DataRecord(
        dbName = batchConfig.database, tbName = batchConfig.table,
        rDate = batchConfig.partitionDate, clusterId = clusterId,
        dataIndex = index, indexSize = shardCount, hostAddress = hostAddress)
      val verifiedOpt = mgr.verifyDataRecord(template, batchConfig.overwrite)

      if (verifiedOpt.isEmpty) {
        logInfo(s"[ClickHouseConcurrentWriter] Shard $index already succeeded, skipping")
        skippedShards.add(index)
      } else {
        verifiedOpt.foreach { record =>
          verifiedRecords.put(index, record)
          val actualHost = if (record.hostAddress.nonEmpty) record.hostAddress
                           else assignments.getOrElse(index, "")
          logInfo(s"[ClickHouseConcurrentWriter] Shard $index using host: $actualHost " +
            s"(from ${if (record.hostAddress.nonEmpty) "existing record" else "new assignment"})")
          mgr.updateDataVolume(record.copy(originalDataVolume = 0L, insertDataVolume = 0L))
          mgr.updateTaskStatus(record.copy(status = RecordStatus.RUNNING_VALUE))
        }
      }
    }

    def onShardComplete(index: Int, result: BatchWriteResult): Unit =
      Option(verifiedRecords.get(index)).foreach { baseRecord =>
        val actualHost = if (result.hostAddress.nonEmpty) result.hostAddress else baseRecord.hostAddress
        val record = baseRecord.copy(
          hostAddress        = actualHost,
          originalDataVolume = result.sourceRowCount,
          insertDataVolume   = result.rowCount,
          status             = if (result.success) RecordStatus.SUCCESS_VALUE else RecordStatus.FAILURE_VALUE)

        mgr.updateDataVolume(record)
        mgr.updateTaskStatus(record)

        if (!result.success && result.rowCount > 0 && actualHost.nonEmpty) {
          val switchCount = backupSwitchCounts.getOrElse(index, 0)
          if (switchCount >= batchConfig.maxBackupSwitches) {
            logError(s"[ClickHouseConcurrentWriter] Shard $index: max backup switches (${batchConfig.maxBackupSwitches}) reached, " +
              "skipping backup switch")
            if (backupSwitchHosts.contains(index)) {
              logError(s"[ClickHouseConcurrentWriter] Shard $index has backup-switch history")
            }
          } else {
            logWarning(
              s"[ClickHouseConcurrentWriter] Shard $index: clearing ${result.rowCount} partial row(s)")
            val cleared = Try {
              val actualJdbcUrl = ClickHouseConcurrentWriter.replaceHost(batchConfig.jdbcUrl, actualHost)
              ensureDataCleared(actualJdbcUrl, batchConfig.table, batchConfig.partitionDate,
                                batchConfig.partitionColumn, batchConfig.options)
            }.recover {
              case e: Exception =>
                logError(
                  s"[ClickHouseConcurrentWriter] Shard $index: partial-data cleanup failed",
                  LogRedaction.sanitizedThrowable(e))
                false
            }.getOrElse(false)

            if (cleared) {
              mgr.updateDataVolume(record.copy(insertDataVolume = 0L))
              Try(machineMgr.getBackupMachine(actualHost)).foreach { backup =>
                val newCount = switchCount + 1
                backupSwitchCounts(index) = newCount
                backupSwitchHosts.getOrElseUpdate(index, scala.collection.mutable.ArrayBuffer.empty) += actualHost
                logInfo(s"[ClickHouseConcurrentWriter] Shard $index: switching to a backup machine " +
                  s"(switch $newCount/${batchConfig.maxBackupSwitches})")
                assignments(index) = backup
              }
            } else {
              logError(s"[ClickHouseConcurrentWriter] Shard $index: failed to clear partial data, not switching backup machine")
            }
          }
        }
      }

    def getSkippedShardRecord(index: Int): Option[DataRecord] = {
      val template = buildDataRecordTemplate(index, shardCount, clusterId)
      Some(mgr.searchRecord(template).getOrElse(template))
    }

    (Some(machineMgr), assignments, shardCount, effectiveBatchConfig,
     onShardStart, onShardComplete, skippedShards, getSkippedShardRecord)
  }

  // ==================== No Meta Initialization Path ====================

  private def initWithoutMeta(): InitResult = {
    val hostAddress = context.hostAddress.getOrElse(
      throw PistaErrors.clickHouseWriterError(
        "hostAddress is required when meta is not configured (single-machine mode)"))

    val assignments = TrieMap(0 -> hostAddress)
    val skippedShards = ConcurrentHashMap.newKeySet[Int]()

    logInfo("[ClickHouseConcurrentWriter] No-meta mode: single-shard destination configured")

    def onShardStart(index: Int): Unit =
      logInfo(s"[ClickHouseConcurrentWriter] Shard $index starting (no-meta mode)")

    def onShardComplete(index: Int, result: BatchWriteResult): Unit = {
      if (result.success)
        logInfo(s"[ClickHouseConcurrentWriter] Shard $index completed: ${result.rowCount} rows written")
      else
        logWarning(s"[ClickHouseConcurrentWriter] Shard $index failed")
    }

    def getSkippedShardRecord(index: Int): Option[DataRecord] = None

    (None, assignments, 1, batchConfig,
     onShardStart, onShardComplete, skippedShards, getSkippedShardRecord)
  }

  // ==================== Validate ====================

  /**
   * Compare aggregate ClickHouse row counts with the source count after all shards finish.
   *
   * - With metadata: aggregate data volume from metadata summary and update the final state
   * - No metadata: converged to log-level comparison (exact validation done internally in ClickHouseBatchWriter.write())
   */
  def validateDataVolume(totalInserted: Long, actualMachineCount: Int, allSucceeded: Boolean): Unit = {
    if (totalInserted != 0) {
      context.metaManager match {
      case Some(mgr) =>
        verifiedDataInfoRecord match {
          case None =>
            logWarning("[ClickHouseConcurrentWriter] No verified DataInfoRecord found, " +
              "skipping final status update")

          case Some(dataInfoRecord) =>
            val summedVolume = mgr.sumDataVolume(dataInfoRecord)

            logInfo(s"[ClickHouseConcurrentWriter] Data volume check: " +
              s"source=$totalInserted, ck-sum=$summedVolume")
            if (summedVolume != totalInserted)
              throw PistaErrors.clickHouseWriterError(
                s"Data volume mismatch: ck-actual=$summedVolume != source=$totalInserted")

            val finalRecord = dataInfoRecord.copy(
              dataVolume = summedVolume,
              status     = if (allSucceeded) RecordStatus.SUCCESS_VALUE else RecordStatus.FAILURE_VALUE
            )
            mgr.updateDataVolume(finalRecord)
            mgr.updateTaskStatus(finalRecord)
            logInfo(s"[ClickHouseConcurrentWriter] DataInfoRecord final status: " +
              s"${finalRecord.status}, dataVolume=${finalRecord.dataVolume}, id=${finalRecord.id}")
        }

      case None =>
        logInfo(s"[ClickHouseConcurrentWriter] No-meta mode: wrote $totalInserted rows total " +
          s"(per-shard verification done in ClickHouseBatchWriter)")
      }
    }
  }

  // ==================== Internal Utility Methods ====================

  private val COLUMNAR_FORMATS = Set("parquet", "orc")
  private val NO_COMPRESSION   = Set("none", "uncompressed")

  private def prepartitionIfConfigured(df: DataFrame): (DataFrame, Option[CheckpointHandle]) =
    context.prepartitionDir match {
      case None =>
        logInfo("[ClickHouseConcurrentWriter] Pre-partition disabled (prepartition.dir not set)")
        (df, None)
      case Some(dir) =>
        val fmt  = context.prepartitionFormat.toLowerCase
        val comp = context.prepartitionCompression.toLowerCase

        if (!COLUMNAR_FORMATS.contains(fmt))
          throw new IllegalArgumentException(
            s"[ClickHouseConcurrentWriter] prepartition.format must be columnar (parquet/orc), got: $fmt")
        if (NO_COMPRESSION.contains(comp))
          throw new IllegalArgumentException(
            s"[ClickHouseConcurrentWriter] prepartition.compression is required, got: $comp")

        // Print execution plan diagnostic information (plan analysis does not trigger Action)
        val (aqeActive, hasExchange, exchangeNames) = inspectPlan(df)
        val stats    = df.queryExecution.optimizedPlan.stats
        val estBytes = stats.sizeInBytes
        val estRows  = stats.rowCount.map(_.toString).getOrElse("unknown")
        logInfo(s"[ClickHouseConcurrentWriter] Plan inspection: " +
          s"aqeActive=$aqeActive, hasExchange=$hasExchange, exchanges=[${exchangeNames.mkString(", ")}], " +
          s"estimatedBytes=$estBytes, estimatedRows=$estRows")

        if (!hasExchange) {
          logInfo("[ClickHouseConcurrentWriter] Skipping pre-partition: no Shuffle Exchange in plan " +
            "(pure FileScan query, AQE coalesce has no effect)")
          (df, None)
        } else {
          logInfo(s"[ClickHouseConcurrentWriter] Pre-partition start: " +
            s"dir=$dir, format=$fmt, compression=$comp")
          MaterializationManager.initialize(spark, dir)
          val startMs = System.currentTimeMillis()
          val handle  = MaterializationManager.checkpoint(df, format = fmt, compression = comp)
          val elapsed = System.currentTimeMillis() - startMs
          logInfo(s"[ClickHouseConcurrentWriter] Pre-partition done: " +
            s"path=${handle.path}, partitions=${handle.dataFrame.rdd.getNumPartitions}, elapsed=${elapsed}ms")
          (handle.dataFrame, Some(handle))
        }
    }

  /**
   * Return whether AQE is active, whether the plan contains Exchange, and the Exchange node names.
   *
   * When AQE is active, Exchange nodes are absent from QueryExecution.sparkPlan (the planner output),
   * it is instead handled by AdaptiveSparkPlanExec internal queryStagePreparationRuleswithin EnsureRequirements)
   * Write initialPlan. AdaptiveSparkPlanExec.executedPlan(currentPhysicalPlan)
   * Inspect the executed plan after the action so adaptive Exchange nodes are visible.
   *
   * AQE inactive, use QueryExecution.executedPlan (also a SparkPlan, no AQE encapsulation).
   */
  private def inspectPlan(df: DataFrame): (Boolean, Boolean, Seq[String]) = {
    val qeExecutedPlan = df.queryExecution.executedPlan
    val aqeActive      = qeExecutedPlan.isInstanceOf[AdaptiveSparkPlanExec]
    val planToInspect  = qeExecutedPlan match {
      case aqe: AdaptiveSparkPlanExec => aqe.executedPlan  // initialPlan: EnsureRequirements applied
      case other                      => other
    }
    val exchanges = planToInspect.collect { case e: Exchange => e.getClass.getSimpleName }
    (aqeActive, exchanges.nonEmpty, exchanges)
  }

  private def buildDataRecordTemplate(index: Int, shardCount: Int, clusterId: Int) =
    DataRecord(
      dbName = batchConfig.database, tbName = batchConfig.table,
      rDate = batchConfig.partitionDate, clusterId = clusterId,
      dataIndex = index, indexSize = shardCount)

  private def buildDataInfoRecord(count: Int, clusterId: Int) = DataInfoRecord(
    dbName    = batchConfig.database,
    tbName    = batchConfig.table,
    rDate     = batchConfig.partitionDate,
    clusterId = clusterId,
    indexSize = count
  )

  /**
   * Overwrite existing data in the ClickHouse table with the current data when there is meta.
   * Entry nodes and machine lists are retrieved from the meta database, and fallback to localhost is prohibited.
   */
  private def deleteClickHouseData(mgr: MetaManager, clusterName: String, clusterId: Int, mode: String): Unit = {
    val machines = mgr.getMachinesByClusterId(clusterId)
    if (machines.isEmpty)
      throw PistaErrors.clickHouseWriterError(
        s"No available machines in cluster $clusterName (clusterId=$clusterId)")

    mode match {
      case "on_cluster" =>
        val entryHost = machines.head
        val entryUrl  = ClickHouseConcurrentWriter.replaceHost(batchConfig.jdbcUrl, entryHost)
        val onClusterSql = if (batchConfig.partitionDate.nonEmpty)
          s"ALTER TABLE ${batchConfig.database}.${batchConfig.table} ON CLUSTER $clusterName " +
            s"DROP PARTITION '${batchConfig.partitionDate}'"
        else
          s"TRUNCATE TABLE ${batchConfig.database}.${batchConfig.table} ON CLUSTER $clusterName"
        logInfo(s"[ClickHouseConcurrentWriter] Executing ON CLUSTER via $entryHost: $onClusterSql")
        executeSql(entryUrl, batchConfig.options, onClusterSql)
        logInfo(s"[ClickHouseConcurrentWriter] Successfully deleted data using ON CLUSTER")

      case "per_host" =>
        val deleteSql = if (batchConfig.partitionDate.nonEmpty)
          s"ALTER TABLE ${batchConfig.database}.${batchConfig.table} " +
            s"DROP PARTITION '${batchConfig.partitionDate}'"
        else
          s"TRUNCATE TABLE ${batchConfig.database}.${batchConfig.table}"
        logInfo(s"[ClickHouseConcurrentWriter] Executing on ${machines.size} hosts: $deleteSql")
        machines.foreach { host =>
          val url = ClickHouseConcurrentWriter.replaceHost(batchConfig.jdbcUrl, host)
          logInfo(s"[ClickHouseConcurrentWriter] Deleting on host: $host")
          executeSql(url, batchConfig.options, deleteSql)
        }
        logInfo(s"[ClickHouseConcurrentWriter] Successfully deleted data on ${machines.size} hosts")
    }
  }

  /** Execute SQL statement (clickhouse.username/password → JDBC user/password conversion) */
  private def executeSql(jdbcUrl: String, opts: Map[String, String], sql: String): Unit = {
    val props = new java.util.Properties()
    opts.foreach { case (k, v) => props.setProperty(k, v) }
    val (user, password) = getJdbcAuth(opts)
    props.setProperty("user", user)
    props.setProperty("password", password)
    val conn = try
      java.sql.DriverManager.getConnection(jdbcUrl, props)
    catch {
      case e: Exception =>
        throw PistaErrors.clickHouseWriterError(s"Failed to connect to ClickHouse: ${e.getMessage}", e)
    }
    try {
      val stmt = conn.createStatement()
      try stmt.executeUpdate(sql)
      finally stmt.close()
    } finally conn.close()
  }

  /**
   * Ensure data is empty: delete first and then poll for validation.
   * Check frequency and interval read from batchConfig (default is 30 times × 10 seconds = 5 minutes).
   */
  private def ensureDataCleared(
    jdbcUrl: String,
    tbl: String,
    datePartition: String,
    partCol: String,
    opts: Map[String, String]
  ): Boolean = {
    val maxAttempts = batchConfig.clearDataMaxAttempts
    val intervalMs  = batchConfig.clearDataIntervalMs

    logInfo(s"[ClickHouseConcurrentWriter] ensureDataCleared: starting for table $tbl, " +
      s"maxAttempts=$maxAttempts, intervalMs=${intervalMs}ms")
    clearPartitionViaJdbc(jdbcUrl, tbl, datePartition, opts)

    val countCondition =
      if (datePartition.nonEmpty && partCol.nonEmpty) s"$partCol = '$datePartition'"
      else ""

    var cleared = false
    for (i <- Range.inclusive(1, maxAttempts) if !cleared) {
      cleared = countRowsViaJdbc(jdbcUrl, tbl, opts, countCondition) == 0L
      if (cleared) {
        logInfo(s"[ClickHouseConcurrentWriter] ensureDataCleared: " +
          s"data cleared after $i attempts (${i * intervalMs / 1000}s)")
      } else {
        logWarning(s"[ClickHouseConcurrentWriter] ensureDataCleared: " +
          s"attempt $i/$maxAttempts, data not cleared yet, waiting ${intervalMs}ms...")
        Thread.sleep(intervalMs)
      }
    }

    if (!cleared)
      logError(s"[ClickHouseConcurrentWriter] ensureDataCleared: " +
        s"timeout after $maxAttempts attempts (${maxAttempts * intervalMs / 1000}s), data not cleared")
    cleared
  }
}

/** JDBC endpoint replacement shared by the writer and endpoint compatibility tests. */
private[batch] object ClickHouseConcurrentWriter extends Logging {
  def replaceHost(jdbcUrl: String, newHost: String): String =
    try {
      val uri = new URI(jdbcUrl.replaceFirst("^jdbc:", ""))
      val (host, explicitPort) = parseHostPort(newHost)
      val port = explicitPort.orElse(if (uri.getPort > 0) Some(uri.getPort) else None)
        .map(value => s":$value").getOrElse("")
      val query = if (uri.getRawQuery != null) s"?${uri.getRawQuery}" else ""
      val formattedHost = if (host.contains(":") && !host.startsWith("[")) s"[$host]" else host
      s"jdbc:${uri.getScheme}://$formattedHost$port${uri.getRawPath}$query"
    } catch {
      case _: URISyntaxException | _: IllegalArgumentException =>
        logWarning("[ClickHouseConcurrentWriter] Failed to replace the configured JDBC host")
        jdbcUrl
    }

  private def parseHostPort(value: String): (String, Option[Int]) = {
    val trimmed = value.trim
    if (trimmed.startsWith("[")) {
      val end = trimmed.indexOf(']')
      if (end > 0 && trimmed.length > end + 2 && trimmed.charAt(end + 1) == ':')
        (trimmed.substring(1, end), Some(trimmed.substring(end + 2).toInt))
      else (trimmed.stripPrefix("[").stripSuffix("]"), None)
    } else if (trimmed.count(_ == ':') == 1) {
      val separator = trimmed.lastIndexOf(':')
      val host = trimmed.substring(0, separator)
      val port = trimmed.substring(separator + 1)
      if (host.nonEmpty && port.forall(_.isDigit)) (host, Some(port.toInt))
      else (trimmed, None)
    } else (trimmed, None)
  }
}
