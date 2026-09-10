package com.pista.spark.sql.execution.datasources.writer

import com.pista.spark.sql.execution.datasources.ConnectorSupport
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

/**
 * Connector write support trait
 * Provides common connector write functionality for databases
 *
 */
trait ConnectorWriteSupport extends ConnectorSupport {
  // Self-type: classes mixing in this trait must extend AbstractDataWriter.
  self: AbstractDataWriter with JdbcWriteSupport =>

  /**
   * Apply all configuration options to DataFrameWriter
   * @param writer DataFrameWriter
   * @param options Configuration options
   * @return Configured DataFrameWriter
   */
  private def applyOptions(writer: DataFrameWriter[Row], options: Map[String, String]): DataFrameWriter[Row] =
    options.foldLeft(writer) {
      case (r, (key, value)) => r.option(key, value)
    }

  /**
   * Connector write (subclass implements)
   * @param df DataFrame
   * @param config Output configuration
   */
  protected def writeViaConnector(df: DataFrame, config: OutputConfig): Unit

  /**
   * Execute auxiliary method for JDBC write
   * @param df DataFrame
   * @param stats statistics
   * @param config Output configuration
   * @param reason use JDBC the reason (for logging)
   */
  private def performJdbcWrite(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig,
    reason: String
  ): Unit = {
    logInfo(s"[$engineName] $reason")

    val path = config.path.get
    val jdbcUrl = buildJdbcUrl(path, config.options)
    // todo Implement extractTableName to avoid double inheritance with JdbcReadSupport.extractTableName
//    val tableName = extractTableName(path)
    val tableName = if (path.contains(".")) path.split("\\.").last else path
    val batchSize = recommendedBatchSize(stats)
    val properties = buildJdbcProperties(config.options)

    writeViaJdbc(df, jdbcUrl, tableName, batchSize, properties, config.mode)
  }

  /**
   * core write logic template method
   * Choose JDBC or Connector mode automatically based on data volume
   */
  protected def writeInternalTemplate(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig
  ): Unit = {
    // Check if JDBC is forced.
    val forceJdbc = getOption(config.options, "force.jdbc", "false").toLowerCase == "true"

    if (forceJdbc) {
      // Force JDBC usage
      performJdbcWrite(df, stats, config, "Force using JDBC (force.jdbc=true)")

    } else if (stats.rowCount.exists(_ < smallDataThreshold)) {
      // Small dataset, use JDBC
      val rowCount = stats.rowCount.get
      performJdbcWrite(df, stats, config, s"Using JDBC for small data ($rowCount rows < $smallDataThreshold)")

    } else {
      // Large dataset or unknown, use Connector
      val rowInfo = stats.rowCount.map(r => s"$r rows >= $smallDataThreshold").getOrElse("unknown size")
      logInfo(s"[$engineName] Using Connector for large data ($rowInfo)")

      writeViaConnector(df, config)
    }
  }

  /**
   * Connector write template method
   * @param df DataFrame
   * @param config Output configuration
   * @param format Connector Format Name (e.g., "doris", "clickhouse")
   * @param tableOptionKey Table configuration key (such as "doris.table.identifier", "table")
   * @param extraOptions Additional configuration options (optional)
   */
  protected def writeViaConnectorTemplate(
    df: DataFrame,
    config: OutputConfig,
    format: String,
    tableOptionKey: String,
    extraOptions: Map[String, String] = Map.empty
  ): Unit = {
    val path = config.path.get
    logInfo(s"[$engineName] Writing via configured Connector destination")

    var writer = df.write.format(format).mode(config.mode).option(tableOptionKey, normalizeTableIdentifier(path))

    // Apply user configuration options
    writer = applyOptions(writer, config.options)

    // Apply additional options (which will override user configurations)
    writer = applyOptions(writer, extraOptions)

    // Apply partition
    if (config.partitionBy.nonEmpty) {
      logInfo(s"[$engineName] Partitioning by: ${config.partitionBy.mkString(", ")}")
      writer = writer.partitionBy(config.partitionBy: _*)
    }

    writer.save()
    logInfo(s"[$engineName] Successfully wrote via Connector")
  }
}
