package com.pista.spark.sql.execution.datasources.reader

import com.pista.spark.sql.execution.datasources.ConnectorSupport
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Connector read support trait
 * Provides common connector read functionality for databases
 *
 */
trait ConnectorReadSupport extends ConnectorSupport {
  // Self-type: classes mixing in this trait must extend AbstractDataReader.
  self: AbstractDataReader with JdbcReadSupport =>

  /**
   * Apply all configuration options to DataFrameReader
   * @param reader DataFrameReader
   * @param options Configuration options
   * @return Configured DataFrameReader
   */
  protected def applyOptions(reader: DataFrameReader, options: Map[String, String]): DataFrameReader =
    options.foldLeft(reader) {
      case (r, (key, value)) => r.option(key, value)
    }

  /**
   * Connector reads (subclass implementation)
   * @param spark SparkSession
   * @param schema Optional Schema
   * @param config Input configuration
   * @return DataFrame
   */
  protected def readViaConnector(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame

  /**
   * core read logic template method
   * Choose JDBC or Connector mode automatically based on data volume
   */
  protected def readInternalTemplate(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = {
    val jdbcUrl = buildJdbcUrl(config.path.get, config.options)
    val tableName = requireValidIdentifier(extractTableName(config.path.get))
    val properties = buildJdbcProperties(config.options)

    // Check if JDBC is forced.
    val forceJdbc = getOption(config.options, "force.jdbc", "false").toLowerCase == "true"

    if (forceJdbc) {
      // Force JDBC usage
      logInfo(s"[$engineName] Force using JDBC (force.jdbc=true)")
      readViaJdbc(
        spark, jdbcUrl, tableName, properties, schema,
        config.predicates
      )
    } else {
      // Estimate data volume to choose read mode
      val estimatedRows = estimateRowCount(jdbcUrl, tableName, properties)

      if (estimatedRows.exists(_ < smallDataThreshold)) {
        // Small dataset, use JDBC
        logInfo(s"[$engineName] Using JDBC for small data (${estimatedRows.get} rows < $smallDataThreshold)")
        readViaJdbc(
          spark, jdbcUrl, tableName, properties, schema,
          config.predicates
        )
      } else {
        // Large dataset or unknown, use Connector
        val rowInfo = estimatedRows.map(r => s"$r rows >= $smallDataThreshold").getOrElse("unknown size")
        logInfo(s"[$engineName] Using Connector for large data ($rowInfo)")
        readViaConnector(spark, schema, config)
      }
    }
  }

  /**
   * Connector reads the template method
   * @param spark SparkSession
   * @param schema Optional Schema
   * @param config Input configuration
   * @param format Connector Format Name (e.g., "doris", "clickhouse")
   * @param tableOptionKey Table configuration key (such as "doris.table.identifier", "table")
   * @param extraOptions Additional configuration options (optional)
   * @return DataFrame
   */
  protected def readViaConnectorTemplate(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig,
    format: String,
    tableOptionKey: String,
    extraOptions: Map[String, String] = Map.empty
  ): DataFrame = {
    val path = config.path.get
    logInfo(s"[$engineName] Reading via configured Connector destination")

    var reader = spark.read.format(format)

    schema.foreach(s => reader = reader.schema(s))

    reader = reader.option(tableOptionKey, normalizeTableIdentifier(path))

    // Apply user configuration options
    reader = applyOptions(reader, config.options)

    // Apply additional options (which will override user configurations)
    reader = applyOptions(reader, extraOptions)

    reader.load()
  }
}
