package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import org.apache.spark.internal.Logging

/**
 * ClickHouse JDBC layer support
 *
 * Provide the foundational JDBC capabilities required for the ClickHouse SPI (Reader/Writer) scenario:
 * - JDBC URL building
 * - JDBC Properties build
 * - Authentication Information Extraction
 * - configuration validation
 *
 * Distinction from ClickHouseBatchJdbcSupport:
 * This trait serves only the SPI layer and does not contain any DataFrame write operations.
 * ClickHouseWriter Inherits this trait to acquire JDBC capabilities accompanying the SPI.
 *
 */
trait ClickHouseJdbcSupport extends Logging {

  /**
   * Build ClickHouse JDBC URL
   *
   * @param path JDBC URL or table path (e.g., db.table)
   * @param options Configuration options
   * @param connectionParams JDBC URL parameters, for example http_connection_provider
   * @return JDBC URL
   */
  protected def buildJdbcUrl(
    path: String,
    options: Map[String, String],
    connectionParams: Map[String, String] = Map.empty
  ): String = {
    if (path.startsWith("jdbc:")) path
    else {
      // Read host and port from clickhouse.* or unprefixed output options.
      val host = options.getOrElse("clickhouse.host",
        options.getOrElse("host",
          throw new IllegalArgumentException(
            "clickhouse.host is required when not using a full JDBC URL (jdbc:clickhouse://...)")))
      val port = options.getOrElse("clickhouse.port", options.getOrElse("port", "8123"))
      val database = if (path.contains(".")) path.split("\\.")(0) else "default"

      val params = if (connectionParams.nonEmpty)
        "?" + connectionParams.map { case (k, v) => s"$k=$v" }.mkString("&")
      else ""

      s"jdbc:clickhouse://$host:$port/$database$params"
    }
  }

  /**
   * Build ClickHouse JDBC Properties
   *
   * @param options Configuration options
   * @return JDBC Properties
   */
  protected def buildClickHouseJdbcProperties(options: Map[String, String]): java.util.Properties = {
    val properties = new java.util.Properties()
    properties.setProperty("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    properties.setProperty("http_connection_provider", "HTTP_URL_CONNECTION")
    val (user, password) = getJdbcAuth(options)
    properties.setProperty("user", user)
    properties.setProperty("password", password)

    options.get("socket_timeout").foreach { timeout =>
      properties.setProperty("socket_timeout", timeout)
    }

    properties
  }

  /**
   * Obtain ClickHouse authentication information*
   *
   * Support two naming conventions, compatible with SPI unified prefix and direct configuration:
   * - SPI Prefix Style (recommended): clickhouse.username / clickhouse.password
   * - Direct style (ClickHouse JDBC standard): user / password
   *
   * @param options Configuration options
   * @return (username, password)
   */
  protected def getJdbcAuth(options: Map[String, String]): (String, String) = {
    val user = options.get("clickhouse.username")
      .orElse(options.get("clickhouse.user"))
      .orElse(options.get("user"))
      .getOrElse("default")
    val password = options.get("clickhouse.password")
      .orElse(options.get("password"))
      .getOrElse("")
    (user, password)
  }

  /**
   * Validate ClickHouse configuration
   *
   * @param path Path
   * @param options Configuration options
   * @return validation result
   */
  protected def validateClickHouseConfig(
    path: Option[String],
    options: Map[String, String]
  ): ValidationResult = {
    if (path.isEmpty)
      ValidationResult(valid = false, Some("Path is required"))
    else {
      // Support two formats:
      // 1. Complete JDBC URL: jdbc:clickhouse://host:port/database
      // 2. Simplify path: database.table (requires options containing host/port)database.table(Requires configuration in options .) options in the host/port)
      val isValidJdbcUrl = path.exists(_.startsWith("jdbc:"))
      val isValidSimplePath = path.exists(_.contains("."))

      if (!isValidJdbcUrl && !isValidSimplePath)
        ValidationResult(valid = false,
          Some(s"Path must be either a full JDBC URL (jdbc:clickhouse://host:port/database) or 'database.table' format, got: ${path.get}"))
      else if (isValidSimplePath && !options.contains("clickhouse.host") && !options.contains("host"))
        ValidationResult(valid = false,
          Some("clickhouse.host is required when using 'database.table' path format"))
      else
        ValidationResult(valid = true, None)
    }
  }

  /**
   * Query the storage engine type of the ClickHouse table (such as MergeTree, Distributed, etc.)
   *
   * @param options JDBC connection options
   * @param database Database name
   * @param table Table name
   * @return Engine type string, returns None when query fails (without blocking normal process)
   */
  protected def queryTableEngine(
    options: Map[String, String],
    database: String,
    table: String
  ): Option[String] = {
    val url = s"jdbc:clickhouse://${options.getOrElse("clickhouse.host",
      options.getOrElse("host", "localhost"))}:${options.getOrElse("clickhouse.port",
      options.getOrElse("port", "8123"))}/system"
    try {
      val conn = java.sql.DriverManager.getConnection(url, buildClickHouseJdbcProperties(options))
      try {
        val pstmt = conn.prepareStatement(
          "SELECT engine FROM system.tables WHERE database = ? AND name = ?")
        try {
          pstmt.setString(1, database)
          pstmt.setString(2, table)
          val rs = pstmt.executeQuery()
          try if (rs.next()) Some(rs.getString(1)) else None
          finally rs.close()
        } finally pstmt.close()
      } finally conn.close()
    } catch {
      case e: Exception =>
        logWarning(s"[ClickHouse] Table-engine query failed with ${e.getClass.getSimpleName}")
        None
    }
  }
}
