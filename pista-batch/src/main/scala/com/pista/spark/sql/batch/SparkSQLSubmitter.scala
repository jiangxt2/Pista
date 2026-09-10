package com.pista.spark.sql.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.SubmitterConf
import com.pista.spark.util.SQLTemplateEngine
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.io.File
import java.util

/**
 * Spark SQL Job Submission Entry (Facade)
 *
 * Delegate to three single responsibility components:
 * - [SQLExecutionContext]: Manages context (traceId, UDF, parameter extraction)
 * - [[SQLStatementExecutor]]: SQL Execution + Processor Chain
 * - [OutputRouter]: write batch
 */
object SparkSQLSubmitter extends Logging {
  lazy val sparkSession: SparkSession = SparkSession
    .builder
    .enableHiveSupport
    .getOrCreate

  def main(args: Array[String]): Unit = {
    val ctx = SQLExecutionContext.init(sparkSession)

    val sqlFilePath = ctx.conf.get(SubmitterConf.SQL_FILE).getOrElse {
      throw PistaErrors.missingRequiredConfigError(SubmitterConf.SQL_FILE.key)
    }
    submitSQLFile(sqlFilePath, ctx)
  }

  /** Public API: Independent entry scenario, initialize complete context */
  def submitSQL(sqlContent: String,
                params: util.HashMap[String, String],
                typedArgs: Map[String, Any] = Map.empty): Unit = {
    submitSQLWithSession(sparkSession, sqlContent, params, typedArgs)
  }

  /** Execute SQL using provided session to accommodate tests requiring an independent Catalog. */
  private[batch] def submitSQLWithSession(spark: SparkSession,
                                          sqlContent: String,
                                          params: util.HashMap[String, String]): Unit =
    submitSQLWithSession(spark, sqlContent, params, Map.empty)

  private[batch] def submitSQLWithSession(spark: SparkSession,
                                          sqlContent: String,
                                          params: util.HashMap[String, String],
                                          typedArgs: Map[String, Any]): Unit = {
    val ctx = SQLExecutionContext.initIfNeeded(spark)
    val processedSQL = SQLTemplateEngine.processSQLTemplate(sqlContent, params)
    SQLStatementExecutor.execute(spark, processedSQL, typedArgs, ctx.conf)
  }

  private[batch] def resolveSQLFilePath(sqlFilePath: String): String = {
    val configuredFile = new File(sqlFilePath)
    if (configuredFile.isFile && configuredFile.canRead) {
      configuredFile.getPath
    } else if (isDistributedFileName(sqlFilePath)) {
      val distributedFile = new File(SparkFiles.get(sqlFilePath))
      if (distributedFile.isFile && distributedFile.canRead)
        distributedFile.getAbsolutePath
      else
        throw PistaErrors.invalidSqlFileError(sqlFilePath)
    } else {
      throw PistaErrors.invalidSqlFileError(sqlFilePath)
    }
  }

  private def isDistributedFileName(path: String): Boolean =
    path.nonEmpty && path != "." && path != ".." &&
      !path.contains('/') && !path.contains('\\')

  private def submitSQLFile(sqlFilePath: String, ctx: SQLExecutionContext): Unit = {
    val resolvedPath = resolveSQLFilePath(sqlFilePath)
    val sqlContent = SQLTemplateEngine.readSQLFile(resolvedPath)
    val processedSQL = SQLTemplateEngine.processSQLTemplate(sqlContent, ctx.freemarkerParams)
    SQLStatementExecutor.execute(sparkSession, processedSQL, ctx.typedArgs, ctx.conf)
  }
}
