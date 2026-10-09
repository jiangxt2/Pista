package com.pista.spark.sql.connector.doris

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.connector.doris.batch._
import com.pista.spark.sql.doris.meta.{DorisFENodeResolver, DorisMetaConf, DorisMetaConnection, DorisMetaManager}
import com.pista.spark.sql.execution.datasources.writer._

import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
 * Doris writer - supports both JDBC and Connector methods
 *
 * Automatically selects the best method based on data size.
 * Connector path integrates DorisConcurrentWriter for Meta tracking,
 * idempotency check, 2PC, and partition overwrite.
 *
 */
class DorisWriter
  extends AbstractDataWriter
  with JdbcWriteSupport
  with ConnectorWriteSupport
  with DorisSupport {

  override def name: String = "doris"
  override protected def engineName: String = "Doris"

  // ========== Core write logic ==========

  override protected def writeInternal(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig
  ): Unit = {
    val conf = ConfigReader(df.sparkSession)
    val overwrite = overwriteRequested(conf, config)

    if (overwrite) {
      logDebug(s"[$engineName] Atomic overwrite requested, selecting Connector mode")
      val transformedDf = transformPartitionColumnIfNeeded(df, conf)
      writeViaConnector(transformedDf, config)
    } else {
      writeInternalTemplate(df, stats, config)
    }
  }

  // ========== Connector Write (integrate DorisConcurrentWriter) ==========

  override protected def writeViaConnector(
    df: DataFrame,
    config: OutputConfig
  ): Unit = {
    val spark = df.sparkSession
    val conf = ConfigReader(spark)

    val metaCtxOpt = tryInitMetaContext(conf)

    try {
      val batchConfig = buildBatchConfig(conf, metaCtxOpt, overwriteRequested(conf, config))
      if (batchConfig.overwrite) validateOverwriteTarget(config, batchConfig)
      val context = DorisConcurrentContext(
        clusterName = conf.getOption(SubmitterConf.DORIS_CLUSTER_NAME.key),
        metaManager = metaCtxOpt.map(_.manager)
      )

      val result = new DorisConcurrentWriter(batchConfig, context, df, System.currentTimeMillis()).write()

      if (!result.success)
        throw PistaErrors.dorisWriterError(result.errorMessage)

    } finally {
      metaCtxOpt.foreach(ctx => Try(ctx.close()))
    }
  }

  // ========== JdbcWriteSupport Interface Implementation ==========

  override protected def jdbcDriver: String = "com.mysql.cj.jdbc.Driver"

  override protected def buildJdbcProperties(options: Map[String, String]): java.util.Properties = {
    val (user, password) = getJdbcAuth(options)
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("rewriteBatchedStatements", "true")
    props.setProperty("cachePrepStmts", "true")
    props.setProperty("group_commit", "async_mode")
    props
  }

  // ========== Helper Methods ==========

  override protected def getJdbcAuth(options: Map[String, String]): (String, String) =
    (getOption(options, "doris.user", "root"),
     getOption(options, "doris.password", ""))

  // ========== CONFIGURATION VALIDATION ==========

  override def validateConfig(config: OutputConfig): ValidationResult = {
    // A non-empty path is the only early check; conf.require in buildBatchConfig enforces fenodes, database, and table.
    // Raise more explicit exceptions to indicate errors clearly.
    // From SparkConf, check here to always falsely report failures.
    if (config.path.isEmpty)
      ValidationResult(valid = false, Some("Output path is required"))
    else
      ValidationResult(valid = true, None)
  }

  // ========== Internal Support ==========

  private def overwriteRequested(conf: ConfigReader, config: OutputConfig): Boolean = {
    val dorisOverwrite = conf.get(SubmitterConf.DORIS_OVERWRITE)
    config.mode.equalsIgnoreCase("overwrite") || dorisOverwrite
  }

  private def validateOverwriteTarget(config: OutputConfig, batchConfig: DorisBatchConfig): Unit = {
    val requested = normalizeTableIdentifier(config.path.getOrElse("").trim, batchConfig.database)
    val configured = s"${batchConfig.database}.${batchConfig.table}"
    if (requested != configured) {
      val reason = "Conflicting Doris overwrite target: spark.pista.output.path must match " +
        "spark.pista.doris.database and spark.pista.doris.table"
      logError(s"[DorisWriter] stage=configuration.target outcome=failed reason=$reason")
      throw PistaErrors.dorisWriterError(reason)
    }
  }

  /** Only a completely unconfigured PostgreSQL store may disable metadata. */
  private def tryInitMetaContext(conf: ConfigReader): Option[MetaContext] = {
    if (conf.getAllWithPrefix("spark.pista.meta.").isEmpty) {
      logInfo("[DorisWriter] Metadata disabled: PostgreSQL metadata is not configured")
      return None
    }

    try {
      val missing = DorisMetaConf.missingKeys(conf)
      if (missing.nonEmpty) {
        val reason = s"Incomplete Doris metadata configuration; missing keys: ${missing.mkString(", ")}"
        logError(s"[DorisWriter] stage=metadata.configure outcome=failed reason=$reason")
        throw PistaErrors.dorisWriterError(reason)
      }
      val metaConf = DorisMetaConf.fromConfigReader(conf)
      val conn = new DorisMetaConnection(metaConf)
      Some(MetaContext(conn, new DorisMetaManager(conn, metaConf)))
    } catch {
      case error @ (_: NoClassDefFoundError | _: ExceptionInInitializerError) =>
        val reason = "Doris metadata was requested but pista-doris-meta could not be loaded; check the runtime assembly"
        logError(s"[DorisWriter] stage=metadata.configure outcome=failed reason=$reason")
        throw PistaErrors.dorisWriterError(reason, error)
    }
  }

  /**
   * Build DorisBatchConfig from spark.pista.doris.* settings.
   */
  private def buildBatchConfig(
    conf: ConfigReader,
    metaCtxOpt: Option[MetaContext],
    overwrite: Boolean
  ): DorisBatchConfig = {
    val clusterName = conf.getOption(SubmitterConf.DORIS_CLUSTER_NAME.key)
      .getOrElse("default_cluster")
    val fenodes = new DorisFENodeResolver(metaCtxOpt.map(_.connection)).resolve(
      paramFenodes = conf.getOption(SubmitterConf.DORIS_FENODES.key),
      clusterName  = clusterName
    )

    DorisBatchConfig(
      fenodes           = fenodes,
      database          = conf.require(SubmitterConf.DORIS_DATABASE),
      table             = conf.require(SubmitterConf.DORIS_TABLE),
      user              = conf.get(SubmitterConf.DORIS_USER),
      password          = conf.get(SubmitterConf.DORIS_PASSWORD),
      writeMode         = WriteMode.fromString(conf.get(SubmitterConf.DORIS_WRITE_MODE)),
      labelPrefix       = conf.get(SubmitterConf.DORIS_LABEL_PREFIX),
      outputPartitions  = conf.get(SubmitterConf.DORIS_OUTPUT_PARTITIONS),
      batchSize         = conf.get(SubmitterConf.DORIS_CONNECTOR_BATCH_SIZE),
      dataFormat        = conf.get(SubmitterConf.DORIS_CONNECTOR_FORMAT),
      enable2PC         = conf.get(SubmitterConf.DORIS_CONNECTOR_ENABLE_2PC),
      autoRedirect      = conf.get(SubmitterConf.DORIS_AUTO_REDIRECT),
      benodes           = conf.get(SubmitterConf.DORIS_BENODES),
      sourceTable       = None,
      feQueryPort       = conf.get(SubmitterConf.DORIS_FE_QUERY_PORT),
      overwrite         = overwrite,
      partitionDate     = conf.get(SubmitterConf.DORIS_PARTITION_DATE),
      partitionColumn   = conf.get(SubmitterConf.DORIS_PARTITION_COLUMN)
    )
  }

  /**
   * In partition coverage write scenarios, if the partition column in DataFrame is of StringType (such as yyyyMMdd string in Hive),
   * Doris target table is of DATE type, and an explicit conversion is required to avoid a CANNOT_SAFELY_CAST error when the Connector writes.
   */
  private def transformPartitionColumnIfNeeded(
    df: DataFrame,
    conf: ConfigReader
  ): DataFrame = {
    val partitionColumn = conf.getOption(SubmitterConf.DORIS_PARTITION_COLUMN.key)
    val dateFormat = conf.get(SubmitterConf.DORIS_PARTITION_DATE_FORMAT)

    partitionColumn match {
      case Some(colName) if df.schema.fields.exists(f => f.name == colName && f.dataType == org.apache.spark.sql.types.StringType) =>
        logInfo(s"[$engineName] Casting $colName from String to Date (format=$dateFormat)")
        df.withColumn(colName, org.apache.spark.sql.functions.to_date(df.col(colName), dateFormat))
      case _ => df
    }
  }

  private case class MetaContext(connection: DorisMetaConnection, manager: DorisMetaManager) {
    def close(): Unit = manager.close()
  }
}
