package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.clickhouse.meta.{MetaConf, MetaConnection, MetaManager}
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.connector.clickhouse.batch.{ClickHouseBatchConfig, ClickHouseConcurrentContext, ClickHouseConcurrentWriter}
import com.pista.spark.sql.execution.datasources.writer._

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ClickHouse writer - Write consistently using ClickHouseConcurrentWriter configuration
 *
 * machineCount=1 degraded into multi-machine mode (single partition), sharing the same write capability.
 * Schema Alignment, Null Handling, Collection Formatting, Write Validation, Automatic Retry.
 *
 * Meta Optional Strategies:
 * - machineCount > 1: meta force (including resume from last row and standby machine switch)
 * - machineCount = 1:meta optional (configured for state tracking, otherwise omitted)
 *
 * Configuration Example (Single Machine Simplified):
 * {{{
 *   --conf spark.pista.output.format=clickhouse \
 *   --conf spark.pista.output.path=mydb.mytable \
 *   --conf spark.pista.output.options.clickhouse.host=ck-host-01
 * }}}
 *
 * Configuration Example (Multi-Machine):
 * {{{
 *   --conf spark.pista.output.format=clickhouse \
 *   --conf spark.pista.output.path=jdbc:clickhouse://entry-host:8123/mydb.mytable \
 *   --conf spark.pista.clickhouse.machine.count=3 \
 *   --conf spark.pista.clickhouse.primaryKey=id \
 *   --conf spark.pista.clickhouse.clusterName=my_cluster \
 *   --conf spark.pista.meta.host=pg-host \
 *   --conf spark.pista.meta.database=meta_db \
 *   --conf spark.pista.meta.username=meta_user \
 *   --conf spark.pista.meta.password=meta_pass
 * }}}
 *
 */
class ClickHouseWriter
  extends AbstractDataWriter
  with JdbcWriteSupport
  with ConnectorWriteSupport
  with ClickHouseJdbcSupport {

  override def name: String = "clickhouse"
  override protected def engineName: String = "ClickHouse"

  /**
   * when prepartitionDir is configured, skip AbstractDataWriter.ensureCached(). prepartitionDir skip when prepartitionDir is configured AbstractDataWriter.ensureCached(),
   * Write the original uncached DataFrame directly into writeInternal.
   *
   * SparkClickHouseConcurrentWriter.inspectPlan() can view the actual execution plan (including Exchange),
   * prepartition checkpoint writes before AQE CoalesceShufflePartitions for Exchange to take effect.
   * The checkpoint file is the persistent data source, so an additional persist() is unnecessary.
   */
  override def write(df: DataFrame, config: OutputConfig): Unit = {
    val conf           = ConfigReader(df.sparkSession)
    val prepartitionOn = !config.streamingMode &&
                         conf.get(SubmitterConf.CLICKHOUSE_PREPARTITION_DIR).isDefined
    if (prepartitionOn) {
      val validation = validateConfig(config)
      if (!validation.valid)
        throw PistaErrors.writerValidationError(
          validation.errorMessage.getOrElse("Invalid configuration"))
      writeInternal(df, DataStats.empty, config)
    } else {
      super.write(df, config)
    }
  }

  override protected def writeInternal(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig
  ): Unit = {
    val conf         = ConfigReader(df.sparkSession)
    val machineCount = conf.get(SubmitterConf.CLICKHOUSE_MACHINE_COUNT).max(1)

    // Multi-machine: meta mandatory; single-machine: meta optional (only captures NoClassDefFoundError)
    val metaOpt = if (machineCount > 1) Some(initMetaManagerOrFail(df.sparkSession))
                  else tryInitMetaManager(df.sparkSession)

    // primaryKey / clusterName: required for multi-machine, optional for single-machine
    val primaryKey  = if (machineCount > 1) Some(conf.require(SubmitterConf.CLICKHOUSE_PRIMARY_KEY))
                      else conf.get(SubmitterConf.CLICKHOUSE_PRIMARY_KEY)
    val clusterName = if (machineCount > 1) Some(conf.require(SubmitterConf.CLICKHOUSE_CLUSTER_NAME))
                      else conf.get(SubmitterConf.CLICKHOUSE_CLUSTER_NAME)

    logInfo(s"[$engineName] machineCount=$machineCount, meta=${metaOpt.isDefined}, " +
      s"primaryKey=${primaryKey.getOrElse("N/A")}, clusterName=${clusterName.getOrElse("N/A")}")

    try
      writeViaConcurrentWriter(df, conf, config, machineCount, primaryKey, clusterName, metaOpt)
    finally
      metaOpt match {
        case Some(m) => scala.util.Try(m.close())
        case None    =>
      }
  }

  /**
   * batch write path concurrency
   */
  private def writeViaConcurrentWriter(
    df: DataFrame,
    conf: ConfigReader,
    config: OutputConfig,
    machineCount: Int,
    primaryKey: Option[String],
    clusterName: Option[String],
    metaOpt: Option[MetaManager]
  ): Unit = {
    val path        = config.path.getOrElse("")
    val confDatabase = conf.get(SubmitterConf.CLICKHOUSE_DATABASE)
    val confTable    = conf.get(SubmitterConf.CLICKHOUSE_TABLE)
    val (database, table) =
      if (confDatabase.isDefined && confTable.isDefined) (confDatabase.get, confTable.get)
      else {
        val (parsedDb, parsedTb) = parsePath(path)
        (confDatabase.getOrElse(parsedDb), confTable.getOrElse(parsedTb))
      }

    val port = conf.get(SubmitterConf.CLICKHOUSE_PORT)
    // Prefer the dedicated configuration, then fall back to clickhouse.host or host in output options.
    val host = conf.get(SubmitterConf.CLICKHOUSE_HOST)
      .orElse(config.options.get("clickhouse.host"))
      .orElse(config.options.get("host"))

    logInfo(s"[$engineName] Host resolution: clickhouse.host=${conf.get(SubmitterConf.CLICKHOUSE_HOST).getOrElse("N/A")}, " +
      s"options.clickhouse.host=${config.options.get("clickhouse.host").getOrElse("N/A")}, " +
      s"options.host=${config.options.get("host").getOrElse("N/A")}, final=${host.getOrElse("EMPTY")}")

    // Two paths unified template: use placeholder (replaced by replaceHost at runtime) if with meta, otherwise use real host
    val jdbcUrl = s"jdbc:clickhouse://${if (metaOpt.isDefined) "placeholder" else host.getOrElse("")}:$port/$database?http_connection_provider=HTTP_URL_CONNECTION"
    logInfo(s"[$engineName] JDBC destination configured")

    // Without metadata, pass the host to ConcurrentWriter for direct connection; if not configured, None, initWithoutMeta will fail quickly.
    val hostAddress = if (metaOpt.isEmpty) host else None

    val batchSize        = conf.get(SubmitterConf.CLICKHOUSE_BATCH_SIZE)
    val outputPartitions = conf.get(SubmitterConf.CLICKHOUSE_OUTPUT_PARTITIONS)
    val overwrite        = conf.get(SubmitterConf.CLICKHOUSE_OVERWRITE)
    val overwriteMode    = conf.get(SubmitterConf.CLICKHOUSE_OVERWRITE_MODE)
    val partitionDate    = conf.get(SubmitterConf.CLICKHOUSE_PARTITION_DATE)
    val partitionColumn  = conf.get(SubmitterConf.CLICKHOUSE_PARTITION_COLUMN)
    val convertNull      = conf.get(SubmitterConf.CLICKHOUSE_CONVERT_NULL_TO_DEFAULT)

    val batchConfig = ClickHouseBatchConfig(
      database             = database,
      table                = table,
      jdbcUrl              = jdbcUrl,
      options              = config.options,
      batchSize            = batchSize,
      outputPartitions     = outputPartitions,
      overwrite            = overwrite,
      overwriteMode        = overwriteMode,
      convertNullToDefault = convertNull,
      partitionDate        = partitionDate,
      partitionColumn      = partitionColumn,
      clearDataMaxAttempts = conf.get(SubmitterConf.CLICKHOUSE_CLEAR_DATA_MAX_ATTEMPTS),
      clearDataIntervalMs  = conf.get(SubmitterConf.CLICKHOUSE_CLEAR_DATA_INTERVAL_MS),
      maxBackupSwitches    = conf.get(SubmitterConf.CLICKHOUSE_MAX_BACKUP_SWITCHES)
    )

    val concurrentContext = ClickHouseConcurrentContext(
      metaManager             = metaOpt,
      clusterName             = clusterName,
      machineCount            = machineCount,
      maxThreads              = conf.get(SubmitterConf.CLICKHOUSE_CONCURRENT_MAX_THREADS),
      maxRetries              = conf.get(SubmitterConf.CLICKHOUSE_RETRY_MAX_TIMES),
      hostAddress             = hostAddress,
      prepartitionDir         = conf.get(SubmitterConf.CLICKHOUSE_PREPARTITION_DIR),
      prepartitionFormat      = conf.get(SubmitterConf.CLICKHOUSE_PREPARTITION_FORMAT),
      prepartitionCompression = conf.get(SubmitterConf.CLICKHOUSE_PREPARTITION_COMPRESSION)
    )

    val concurrentWriter = new ClickHouseConcurrentWriter(df, primaryKey, batchConfig, concurrentContext)

    val (results, actualMachineCount) = concurrentWriter.write()
    val succeeded = results.count(_.success)
    val totalRows = results.map(_.rowCount).sum

    logInfo(s"[$engineName] Concurrent write done: $succeeded/$actualMachineCount shards, $totalRows rows")

    val allSucceeded = succeeded == actualMachineCount
    concurrentWriter.validateDataVolume(totalRows, actualMachineCount, allSucceeded)

    if (!allSucceeded)
      throw PistaErrors.clickHouseWriterError(
        s"Only $succeeded/$actualMachineCount shards succeeded: " +
          results.filter(!_.success).map(r => s"shard-${r.index}: ${r.errorMessage}").mkString(", "))
  }

  /**
   * Parse path as (database, table)
   * Support formats: db.table or jdbc:clickhouse://host:port/db.table
   */
  private def parsePath(path: String): (String, String) = {
    val tableName = if (path.startsWith("jdbc:")) {
      val withoutJdbc = path.replaceFirst("^jdbc:", "")
      val parts       = withoutJdbc.split("/")
      if (parts.length > 1) parts.last else ""
    } else path

    if (!tableName.contains("."))
      throw new IllegalArgumentException(
        s"Path must be in format 'database.table', got: $path")

    val Array(db, tb) = tableName.split("\\.", 2)
    (db, tb)
  }

  private def initMetaManagerOrFail(spark: SparkSession): MetaManager =
    try {
      val metaConf   = MetaConf.fromSparkConf(spark.sparkContext.getConf)
      val connection = new MetaConnection(metaConf)
      new MetaManager(connection)
    } catch {
      case _: NoClassDefFoundError | _: ExceptionInInitializerError =>
        throw PistaErrors.clickHouseWriterError(
          "pista-clickhouse-meta not in classpath, required for concurrent mode (machine.count > 1)")
      case e: Exception =>
        throw PistaErrors.clickHouseWriterError(
          s"Failed to init MetaManager: ${e.getMessage}", e)
    }

  /**
   * Meta Cluster Optional:
   * - meta JAR missing (NoClassDefFoundError) → None
   * - meta parameter not configured (NoSuchElementException, conf.get throws an exception when the key does not exist) → None
   * - connection failures and other Exception → Rethrow the exception without silencing degradation.
   */
  private def tryInitMetaManager(spark: SparkSession): Option[MetaManager] =
    try {
      val metaConf   = MetaConf.fromSparkConf(spark.sparkContext.getConf)
      val connection = new MetaConnection(metaConf)
      Some(new MetaManager(connection))
    } catch {
      case _: NoClassDefFoundError | _: ExceptionInInitializerError => None
      case _: NoSuchElementException                                 => None
    }

  override protected def writeViaConnector(
    df: DataFrame,
    config: OutputConfig
  ): Unit = writeViaConnectorTemplate(df, config, "clickhouse", "table")

  override protected def jdbcDriver: String = "com.clickhouse.jdbc.ClickHouseDriver"

  override def validateConfig(config: OutputConfig): ValidationResult = {
    if (config.format.trim.isEmpty)
      return ValidationResult(valid = false, Some("format cannot be empty"))
    ValidationResult(valid = true, None)
  }
}
