package com.pista.spark.sql.execution.datasources.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import java.util.Properties

/**
 * JDBC write support trait
 * Provides common JDBC write functionality for databases
 *
 */
trait JdbcWriteSupport extends Logging {
  // Self-type: classes mixing in this trait must extend AbstractDataWriter.
  self: AbstractDataWriter =>

  /** JDBC Driver Class Name */
  protected def jdbcDriver: String

  /** Build JDBC URL */
  protected def buildJdbcUrl(
    path: String,
    options: Map[String, String],
    connectionParams: Map[String, String] = Map.empty
  ): String

  /** Obtain JDBC authentication information */
  protected def getJdbcAuth(options: Map[String, String]): (String, String)

  /**
   * Build JDBC Properties
   * Subclasses can override this method to add custom parameters (user, password, socket_timeout, etc.).
   * @param options User configuration options
   * @return JDBC Properties
   */
  protected def buildJdbcProperties(options: Map[String, String]): Properties = new Properties()

  /**
   * JDBC Write Helper Method
   * @param df DataFrame
   * @param jdbcUrl JDBC URL
   * @param tableName Table name
   * @param batchSize Batch size
   * @param properties JDBC Properties
   * @param mode Write mode
   */
  protected def writeViaJdbc(
    df: DataFrame,
    jdbcUrl: String,
    tableName: String,
    batchSize: Int,
    properties: Properties,
    mode: String
  ): Unit = {
    logInfo(s"[$engineName] Writing via JDBC with batch size $batchSize")

    df.write
      .mode(mode)
      .option(JDBCOptions.JDBC_DRIVER_CLASS, jdbcDriver)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, batchSize.toString)
      .jdbc(jdbcUrl, tableName, properties)

    logInfo(s"[$engineName] Successfully wrote via JDBC")
  }
}
