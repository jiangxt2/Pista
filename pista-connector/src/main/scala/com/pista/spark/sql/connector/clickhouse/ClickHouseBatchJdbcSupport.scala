package com.pista.spark.sql.connector.clickhouse

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * ClickHouse batch write JDBC support
 *
 * Provide JDBC operation capability required for ClickHouse batch write scenario:
 * - DataFrame Batch Write
 * - Schema read from table
 * - Clean partition cleanup before overwrite write
 *
 * Reuses ClickHouseJdbcSupport for URL, property, and authentication handling.
 * ClickHouseBatchWriter mixes in this trait to obtain JDBC batch-write operations.
 *
 */
trait ClickHouseBatchJdbcSupport extends ClickHouseJdbcSupport {

  /**
   * Write DataFrame to ClickHouse (JDBC Append mode)
   *
   * Use coalesce to control the number of partitions, avoiding too many small files to connect.
   */
  protected def writeDataViaJdbc(
    df: DataFrame,
    jdbcUrl: String,
    table: String,
    batchSize: Long,
    outputPartitions: Int,
    options: Map[String, String] = Map.empty
  ): Unit = {
    logInfo(s"Writing to ClickHouse table $table (batchSize=$batchSize, partitions=$outputPartitions)")
    df.coalesce(outputPartitions).write
      .mode(SaveMode.Append)
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("batchsize", batchSize.toString)
      .jdbc(jdbcUrl, table, buildClickHouseJdbcProperties(options))
  }

  /** read from ClickHouse table Schemacolumn alignment before writing */
  protected def getClickHouseTableSchema(
    jdbcUrl: String,
    table: String,
    options: Map[String, String] = Map.empty
  )(implicit spark: SparkSession): StructType =
    spark.read.jdbc(jdbcUrl, table, buildClickHouseJdbcProperties(options)).schema

  /**
   * Write complete and query local row count of local ClickHouse table
   *
   * Preserves countLines() compatibility semantics to validate write completeness.
   * conditions is an optional WHERE predicate (for example, "dt='20260407'"); empty means the entire table.
   */
  protected def countRowsViaJdbc(
    jdbcUrl: String,
    table: String,
    options: Map[String, String] = Map.empty,
    conditions: String = ""
  ): Long = {
    val where = if (conditions.nonEmpty) s" WHERE $conditions" else ""
    val sql   = s"SELECT COUNT(1) FROM $table$where"
    withStatement(jdbcUrl, options) { stmt =>
      val rs = stmt.executeQuery(sql)
      try if (rs.next()) rs.getLong(1) else 0L
      finally rs.close()
    }
  }

  /**
   * Clean old data before writing by overwriting existing records.
   *
   * - datePartition Non-empty → `ALTER TABLE DROP PARTITION 'date'`
   * - datePartition is null → `TRUNCATE TABLE` (table without partitions)
   *
   * Write before each shard, jdbcUrl should point to the actual node writing to this shard.
   */
  protected def clearPartitionViaJdbc(
    jdbcUrl: String,
    table: String,
    datePartition: String,
    options: Map[String, String] = Map.empty
  ): Unit = {
    val sql =
      if (datePartition.nonEmpty) s"ALTER TABLE $table DROP PARTITION '$datePartition'"
      else s"TRUNCATE TABLE $table"
    logInfo("Clearing target data before overwrite")
    logInfo("Using configured JDBC destination")
    withStatement(jdbcUrl, options)(_.executeUpdate(sql))
  }

  private def withStatement[T](
    jdbcUrl: String,
    options: Map[String, String]
  )(f: java.sql.Statement => T): T = {
    val conn = java.sql.DriverManager.getConnection(jdbcUrl, buildClickHouseJdbcProperties(options))
    try {
      val stmt = conn.createStatement()
      try f(stmt)
      finally stmt.close()
    } finally conn.close()
  }
}
