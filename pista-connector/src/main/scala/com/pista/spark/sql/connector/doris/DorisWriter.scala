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
    val sparkConf = df.sparkSession.sparkContext.getConf
    val overwrite = sparkConf.getOption(SubmitterConf.DORIS_OVERWRITE.key)
      .exists(_.toLowerCase == "true")

    if (overwrite) {
      logInfo(s"[$engineName] overwrite=true, forcing Connector mode")
      val transformedDf = transformPartitionColumnIfNeeded(df, sparkConf)
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
    val sparkConf = spark.sparkContext.getConf
    val conf = ConfigReader(spark)

    val metaCtxOpt = tryInitMetaContext(sparkConf)
    logInfo(s"[$engineName] Meta tracking ${if (metaCtxOpt.isDefined) "enabled" else "disabled"}")

    try {
      val batchConfig = buildBatchConfig(conf, metaCtxOpt)
      val context = DorisConcurrentContext(
        clusterName = conf.getOption(SubmitterConf.DORIS_CLUSTER_NAME.key),
        metaManager = metaCtxOpt.map(_.manager)
      )

      val result = new DorisConcurrentWriter(batchConfig, context, df, System.currentTimeMillis()).write()

      if (!result.success)
        throw PistaErrors.dorisWriterError(result.errorMessage)

      logInfo(s"[$engineName] Write completed: ${result.rowCount} rows")
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

  /**
   * Initialize Meta context (degraded optional).
   *
   * Reference ClickHouseWriter.tryInitMetaManager:
   * - classpath missing → degradation (None)None)
   * - Configuration missing → Degradation (None)
   * - Connection failed → Throw exception (write interrupted)
   */
  private def tryInitMetaContext(sparkConf: org.apache.spark.SparkConf): Option[MetaContext] =
    try {
      val metaConf = DorisMetaConf.fromSparkConf(sparkConf)
      val conn = new DorisMetaConnection(metaConf)
      Some(MetaContext(conn, new DorisMetaManager(conn, metaConf)))
    } catch {
      case _: NoClassDefFoundError | _: ExceptionInInitializerError =>
        logWarning("[DorisWriter] pista-doris-meta not in classpath, Meta tracking disabled")
        None
      case _: NoSuchElementException =>
        logWarning("[DorisWriter] Meta not configured, Meta tracking disabled")
        None
    }

  /**
   * Build DorisBatchConfig from spark.pista.doris.* settings.
   */
  private def buildBatchConfig(
    conf: ConfigReader,
    metaCtxOpt: Option[MetaContext]
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
      overwrite          = conf.get(SubmitterConf.DORIS_OVERWRITE),
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
    sparkConf: org.apache.spark.SparkConf
  ): DataFrame = {
    val partitionColumn = sparkConf.getOption(SubmitterConf.DORIS_PARTITION_COLUMN.key)
    val dateFormat = sparkConf.get(
      SubmitterConf.DORIS_PARTITION_DATE_FORMAT.key,
      SubmitterConf.DORIS_PARTITION_DATE_FORMAT.defaultValue.getOrElse("yyyyMMdd")
    )

    partitionColumn match {
      case Some(colName) if df.schema.fields.exists(f => f.name == colName && f.dataType == org.apache.spark.sql.types.StringType) =>
        logInfo(s"[$engineName] Casting $colName from String to Date (format=$dateFormat)")
        df.withColumn(colName, org.apache.spark.sql.functions.to_date(df.col(colName), dateFormat))
      case _ => df
    }
  }

  private case class MetaContext(connection: DorisMetaConnection, manager: DorisMetaManager) {
    def close(): Unit = { manager.close(); connection.close() }
  }
}
