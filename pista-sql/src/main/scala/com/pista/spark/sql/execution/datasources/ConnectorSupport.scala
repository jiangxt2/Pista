package com.pista.spark.sql.execution.datasources

import org.apache.spark.internal.Logging

/**
 * Connector support trait
 * Provides common utility methods for database connectors (both Reader and Writer)
 *
 */
trait ConnectorSupport extends Logging {

  /**
   * engine name (for logs)
   */
  protected def engineName: String

  /**
   * Small Data Threshold (for JDBC vs Connector Selection)
   * Subclasses can override this method to customize the threshold*
   */
  protected def smallDataThreshold: Long = 100000L

  /**
   * Standardized table identifier (add default database prefix)
   * @param path Table Path
   * @param defaultDatabase Default database name
   * @return Standardized table identifier
   */
  protected def normalizeTableIdentifier(path: String, defaultDatabase: String = "default"): String =
    if (path.contains(".")) path else s"$defaultDatabase.$path"

  /**
   * Get the value of the configuration item (with default value)
   * @param options Configuration options
   * @param key Configuration key
   * @param defaultValue default value
   * @return Configuration value
   */
  protected def getOption(options: Map[String, String], key: String, defaultValue: String): String =
    options.getOrElse(key, defaultValue)

}
