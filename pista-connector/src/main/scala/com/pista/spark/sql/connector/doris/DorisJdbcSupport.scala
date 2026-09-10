package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.DorisBatchConfig
import org.apache.spark.internal.Logging

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * Doris JDBC helper trait*
 *
 * Provide the JDBC foundational capabilities required for batch write scenarios:
 * - JDBC connection builder (via FE MySQL protocol port)
 * - Write-Back Row Count Query (COUNT)
 *
 * Distinction from DorisSupport: DorisSupport validates configuration at the SPI layer.
 * DorisJdbcSupport provides batch write capability, executing JDBC directly.
 *
 * TODO: connection reuse optimization. Currently, each invocation of executeDorisSql / queryDorisCount / queryShowTransaction /
 * queryPartitionRanges create new connections and immediately close them. In the scenario of partition coverage write (10 partitions create 10+
 * Shard connection), introduce connection cache or ThreadLocal reuse with key as (host, port, user).*
 */
trait DorisJdbcSupport extends Logging {

  /**
   * Execute Doris SQL statement (DDL / ADMIN commands), return success.
   * For executing management statements such as `ADMIN SET FRONTEND CONFIG`.
   *
   * @param sql SQL statement
   * @param config Batch write configuration (used for building the connection)
   * @return true = execution successful
   */
  protected def executeDorisSql(sql: String, config: DorisBatchConfig): Boolean = {
    try {
      val conn = buildDorisJdbcConnection(config)
      try {
        conn.createStatement().execute(sql)
        true
      } finally conn.close()
    } catch {
      case e: Exception =>
        logWarning(
          s"[DorisJdbcSupport] SQL execution failed with ${e.getClass.getSimpleName}")
        false
    }
  }

  /**
   * Query the number of rows in the Doris table for write-after-observability logging.
   * when resolvePartitionFilter is non-empty, append a WHERE clause. resolvePartitionFilter append WHERE clause when returning non-null WHERE clause.
   *
   * @return The number of rows retrieved. Return -1L and log a warning if the operation fails.
   */
  protected def queryDorisCount(config: DorisBatchConfig): Long = {
    val whereClause = config.resolvePartitionFilter().map(f => s" WHERE $f").getOrElse("")
    val sql = s"SELECT COUNT(*) FROM `${config.database}`.`${config.table}`$whereClause"
    try {
      val conn = buildDorisJdbcConnection(config)
      try {
        val rs = conn.createStatement().executeQuery(sql)
        if (rs.next()) rs.getLong(1) else 0L
      } finally conn.close()
    } catch {
      case e: Exception =>
        logWarning(
          s"[DorisJdbcSupport] Row-count query failed with ${e.getClass.getSimpleName}")
        -1L
    }
  }

  protected def buildDorisJdbcConnection(
    config: DorisBatchConfig,
    database: Option[String] = None
  ): Connection = {
    val targetDatabase = database.getOrElse(config.database)
    val feHost = config.fenodes.split(",")(0).trim.split(":")(0)
    val url    = s"jdbc:mysql://$feHost:${config.feQueryPort}/$targetDatabase"
    val props  = new Properties()
    props.setProperty("user",     config.user)
    props.setProperty("password", config.password)
    DriverManager.getConnection(url, props)
  }
}
