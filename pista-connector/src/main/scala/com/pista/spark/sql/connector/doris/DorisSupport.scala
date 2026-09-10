package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import org.apache.spark.internal.Logging

/**
 * Doris configuration validation support
 *
 * Provide shared configuration validation logic for Doris Reader and Writer.
 *
 */
trait DorisSupport extends Logging {

  /**
   * Validate required configurations for Doris*
   *
   * @param options Configuration options
   * @return validation result
   */
  protected def validateDorisOptions(options: Map[String, String]): ValidationResult = {
    val requiredKeys = Seq("doris.fenodes", "doris.user")

    requiredKeys
      .find(!options.contains(_))
      .map(key => ValidationResult(valid = false, Some(s"'$key' option is required")))
      .getOrElse(ValidationResult(valid = true, None))
  }

  /**
   * Build Doris Connector authentication options
   *
   * @param options User configuration options
   * @return Map of options containing authentication information
   */
  protected def buildDorisAuthOptions(options: Map[String, String]): Map[String, String] = {
    val user = options.getOrElse("doris.user", "root")
    val password = options.getOrElse("doris.password", "")

    Map(
      "doris.request.auth.user" -> options.getOrElse("doris.request.auth.user", user),
      "doris.request.auth.password" -> options.getOrElse("doris.request.auth.password", password)
    )
  }

  /**
   * Build Doris JDBC URL
   *
   * @param path JDBC URL or table path (e.g., db.table)
   * @param options Configuration options
   * @return JDBC URL
   */
  protected def buildJdbcUrl(
    path: String,
    options: Map[String, String],
    connectionParams: Map[String, String] = Map.empty
  ): String = {
    // If the URL is already JDBC, return it directly.
    if (path.startsWith("jdbc:")) path
    else {
      // Extract the host of the first FE from doris.fenodes
      val feHost = options.getOrElse("doris.fenodes", "localhost:9030")
        .split(",")(0).split(":")(0)

      // Get query port (default 9030)
      val fePort = options.getOrElse("doris.query.port", "9030")

      // Extract database name from path if path is in format of db.table
      val database = if (path.contains(".")) path.split("\\.")(0) else "default"

      s"jdbc:mysql://$feHost:$fePort/$database"
    }
  }

}
