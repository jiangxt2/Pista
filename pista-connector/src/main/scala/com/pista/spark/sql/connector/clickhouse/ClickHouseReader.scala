package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.reader._
import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * ClickHouse reader - supports both JDBC and Connector methods
 *
 * Automatically selects the best method based on data size estimate
 *
 */
class ClickHouseReader
  extends AbstractDataReader
  with JdbcReadSupport
  with ConnectorReadSupport
  with ClickHouseJdbcSupport {

  override def name: String = "clickhouse"
  override protected def engineName: String = "ClickHouse"
  override def supportsPushdown: Boolean = true
  override def supportsPartitionPruning: Boolean = true

  override def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] =
    super[JdbcReadSupport].inferSchema(spark, config)

  override protected def readInternal(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = {
    warnIfDistributedTable(config)
    readInternalTemplate(spark, schema, config)
  }

  override protected def readViaConnector(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = {
    val extraOptions = scala.collection.mutable.Map[String, String]()

    // Apply predicate pushdown (using connector native filter option to avoid concatenating full SQL)
    if (config.predicates.nonEmpty) {
      val filterQuery = config.predicates.mkString(" AND ")
      logInfo(s"[$engineName] Applying configured filter query")
      extraOptions += ("filter" -> filterQuery)
    }

    readViaConnectorTemplate(spark, schema, config, "clickhouse", "table", extraOptions.toMap)
  }

  override protected def jdbcDriver: String = "com.clickhouse.jdbc.ClickHouseDriver"

  /**
   * Detect distributed tables and print OOM risk warnings*
   *
   * ClickHouse Distributed Engine distributes the query to all shards, and results from each shard are processed by the Coordinator node.
   * Aggregate in memory and return. Large data volume may cause Coordinator OOM. This method detects the target table before reading.
   * Is distributed table. If yes, print a WARNING to alert users about the query conditions and data volume.
   *
   * TODO convertDistributedToLocal: Parse the Distributed table by extracting the cluster topology through system.clusters
   * For each local_db/local_table, rewrite the query to parallel read from local shard tables, bypassing Coordinator.
   *   aggregation bottleneck. Implementation: 1) read engine_full from system.tables and parse shard arguments;
   *   2) system.clusters Retrieves a list of shard and replica addresses.
   *   3) For each shard, generate a separate JDBC URL and rewrite the SQL to point to a local table.
   *   4) use Spark JDBC numPartitions for shard-level parallel reads.
   *   Plan Document: pista-clickhouse-connector-reference-plan.md
   */
  private def warnIfDistributedTable(config: InputConfig): Unit =
    config.path.foreach { path =>
      val tableName = extractTableName(path)
      // Extract database: db.table → db; jdbc:clickhouse://host:port/db.table → db
      val database = if (path.startsWith("jdbc:")) {
        val withoutScheme = path.replaceFirst("^jdbc:clickhouse://", "")
        val pathPart = withoutScheme.split("[?]").head.split("/").lastOption.getOrElse("")
        if (pathPart.contains(".")) pathPart.split("\\.").head else "default"
      } else {
        if (path.contains(".")) path.split("\\.").head else "default"
      }
      queryTableEngine(config.options, database, tableName).foreach { engine =>
        if (engine.contains("Distributed")) {
          logWarning(s"[$engineName] Querying Distributed table '$database.$tableName' (engine=$engine) " +
            s"via JDBC — large result sets may cause Coordinator node OOM. " +
            s"Consider adding WHERE filters, configuring max_memory_usage on ClickHouse server, " +
            s"or reading from local tables directly.")
        }
      }
    }

  override def validateConfig(config: InputConfig): ValidationResult =
    validateClickHouseConfig(config.path, config.options)
}
