package com.pista.spark.util

import com.pista.spark.errors.PistaErrors
import freemarker.cache.StringTemplateLoader
import freemarker.template.Configuration
import org.apache.spark.internal.Logging

import java.io.{File, StringWriter}
import java.util
import scala.io.Source

/**
 * SQL template engine
 *
 * Provide SQL file reading, template parameter substitution, and statement splitting functionality.
 * Support batch and streaming processing sharing usage.
 *
 */
object SQLTemplateEngine extends Logging {

  /**
   * Read SQL file
   *
   * @param sqlFilePath SQL file path (supports local filesystem)
   * @return SQL file content
   */
  def readSQLFile(sqlFilePath: String): String = {
    val file = new File(sqlFilePath)
    if (!file.exists() || !file.canRead) {
      throw PistaErrors.invalidSqlFileError(sqlFilePath)
    }

    val source = Source.fromFile(sqlFilePath)
    try
      source
        .getLines()
        .mkString("\n")
    finally
      source.close()
  }

  /**
   * Process SQL template (using Freemarker for parameter substitution)
   *
   * @param sqlContent SQL template content
   * @param params Parameter mapping (such as table_name -> users)
   * @return Processed SQL content
   */
  def processSQLTemplate(sqlContent: String, params: util.HashMap[String, String]): String = {
    try {
      val cfg = new Configuration(Configuration.VERSION_2_3_34)
      val stringLoader = new StringTemplateLoader()
      stringLoader.putTemplate("sqlTemplate", sqlContent)
      cfg.setTemplateLoader(stringLoader)
      val template = cfg.getTemplate("sqlTemplate")
      val out = new StringWriter()
      template.process(params, out)
      out.toString
    } catch {
      case e: Exception =>
        throw PistaErrors.sqlParseError("template", e.getMessage)
    }
  }

  /**
   * Split SQL statements by semicolons.
   *
   * Handle semicolons in comments and strings correctly using a state machine to avoid accidental splits.
   *
   * @param processedSQL Content of processed SQL statements
   * @return Array of SQL statements (filtered for empty statements and comments only)
   */
  def splitStatements(processedSQL: String): Array[String] = {
    SQLStatementSplitter.split(processedSQL)
  }

  /**
   * Read SQL file and process template
   *
   * @param sqlFilePath SQL file path
   * @param params Parameter mapping
   * @return Processed SQL content
   */
  def readAndProcess(sqlFilePath: String, params: util.HashMap[String, String]): String = {
    val sqlContent = readSQLFile(sqlFilePath)
    processSQLTemplate(sqlContent, params)
  }
}
