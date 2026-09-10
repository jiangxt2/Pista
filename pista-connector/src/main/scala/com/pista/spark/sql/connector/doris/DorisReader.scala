package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.execution.datasources.reader._
import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Doris reader - supports both JDBC and Connector methods
 *
 * Automatically selects the best method based on data size estimate
 *
 */
class DorisReader
  extends AbstractDataReader
  with JdbcReadSupport
  with ConnectorReadSupport
  with DorisSupport {

  override def name: String = "doris"
  override protected def engineName: String = "Doris"
  override def supportsPushdown: Boolean = true
  override def supportsPartitionPruning: Boolean = true

  override def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] =
    super[JdbcReadSupport].inferSchema(spark, config)

  override protected def readInternal(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = readInternalTemplate(spark, schema, config)

  override protected def readViaConnector(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = {
    val feNodes = getOption(config.options, "doris.fenodes", "unknown")
    val user = getOption(config.options, "doris.user", "unknown")
    logInfo(s"[$engineName] Connector endpoint and credentials configured")

    // Doris Connector HTTP API Authentication Parameters
    val authOptions = buildDorisAuthOptions(config.options)
    val extraOptions = scala.collection.mutable.Map.empty[String, String] ++= authOptions

    // Apply Predicate Pushdown
    if (config.predicates.nonEmpty) {
      val filterQuery = config.predicates.mkString(" AND ")
      logInfo(s"[$engineName] Applying configured filter query")
      extraOptions += ("doris.filter.query" -> filterQuery)
    }

    // Apply column pruning
    if (config.columns.nonEmpty) {
      val readFields = config.columns.mkString(",")
      logInfo(s"[$engineName] Applying column projection: $readFields")
      extraOptions += ("doris.read.field" -> readFields)
    }

    readViaConnectorTemplate(spark, schema, config, "doris", "doris.table.identifier", extraOptions.toMap)
  }

  override protected def jdbcDriver: String = "com.mysql.cj.jdbc.Driver"

  override protected def buildJdbcProperties(options: Map[String, String]): java.util.Properties = {
    val (user, password) = getJdbcAuth(options)
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("useSSL", "false")
    props
  }

  override protected def getJdbcAuth(options: Map[String, String]): (String, String) =
    (getOption(options, "doris.user", "root"),
     getOption(options, "doris.password", ""))

  override def validateConfig(config: InputConfig): ValidationResult = {
    if (config.path.isEmpty)
      ValidationResult(valid = false, Some("Input path is required"))
    else
      validateDorisOptions(config.options)
  }
}
