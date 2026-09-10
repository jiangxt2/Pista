package com.pista.spark.sql.batch

import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.metrics.MetricsManager
import com.pista.spark.sql.functions.PistaFunctionInstaller
import com.pista.spark.util.ParamTypeParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.MDC

import java.util

/**
 * SQL Execution Context: SparkSession Configuration, traceId, UDF Registration, and Parameter Extraction
 *
 * Extract from SparkSQLSubmitter, with a single responsibility. UDF registration is unified here.
 */
class SQLExecutionContext private[batch] (
    val spark: SparkSession,
    val conf: ConfigReader,
    val freemarkerParams: util.HashMap[String, String],
    val typedArgs: Map[String, Any])

object SQLExecutionContext extends Logging {

  /** main() entry: complete initialization (traceId + UDF + validate + parameter extraction + metric collection) */
  def init(spark: SparkSession): SQLExecutionContext = {
    val conf = ConfigReader(spark)
    SubmitterConf.validate(conf)

    // Generate traceId, spanning the lifecycle of the job lifecycle
    val traceId = java.util.UUID.randomUUID().toString.take(8)
    MDC.put("traceId", traceId)
    spark.conf.set("spark.pista.traceId", traceId)

    configureLogLevel(spark, conf)

    // Initialize metrics collection
    MetricsManager.initialize(spark)

    // UDF register (unique registration point)
    PistaFunctionInstaller.install(spark)

    val (freemarkerParams, typedArgs) = extractParameters(conf)

    new SQLExecutionContext(spark, conf, freemarkerParams, typedArgs)
  }

  /**
   * submitSQL() PUBLIC ENTRY:
   * - traceId if exists reuse, otherwise generate
   * - Register UDF + validate + extract parameters executed each time
   * (with the current ) submitSQL(public) L520-536 behavior consistent)
   */
  def initIfNeeded(spark: SparkSession): SQLExecutionContext = {
    // traceId exists for reuse, otherwise generate new one
    if (MDC.get("traceId") == null) {
      val traceId = java.util.UUID.randomUUID().toString.take(8)
      MDC.put("traceId", traceId)
      spark.conf.set("spark.pista.traceId", traceId)
    }

    val conf = ConfigReader(spark)
    SubmitterConf.validate(conf)

    // UDF register
    PistaFunctionInstaller.install(spark)

    val (freemarkerParams, typedArgs) = extractParameters(conf)

    new SQLExecutionContext(spark, conf, freemarkerParams, typedArgs)
  }

  private def configureLogLevel(spark: SparkSession, conf: ConfigReader): Unit = {
    val logLevel = conf.get(SubmitterConf.LOG_LEVEL)
    spark.sparkContext.setLogLevel(logLevel)
  }

  /**
   * Extract parameters into Freemarker template parameters and typed parameters.
   *
   * Parameters are passed in via spark.pista.params.* configuration:
   * - All parameters are as Freemarker template parameters (String type)
   * - Parameters with a `.type` suffix are also parsed as typed values for Spark parameterized queries.
   * - parameters without a .type declaration are also typed as strings. .type parameters declared as type will also be treated as String typed parameters of type
   */
  private def extractParameters(conf: ConfigReader): (util.HashMap[String, String], Map[String, Any]) = {
    val freemarkerParams = new util.HashMap[String, String]()
    val parameterizedEnabled = conf.get(SubmitterConf.PARAMETERIZED_ENABLED)

    // collect all parameters. params.* configuration
    val allParams = conf.getAllWithPrefix(SubmitterConf.PARAMS_FULL_PREFIX)

    // Separate the type declaration and parameters
    val typeDeclarations = allParams.filter(_._1.endsWith(SubmitterConf.PARAMS_TYPE_SUFFIX))
      .map { case (k, v) => k.stripSuffix(SubmitterConf.PARAMS_TYPE_SUFFIX) -> v }
    val paramValues = allParams.filterNot(_._1.endsWith(SubmitterConf.PARAMS_TYPE_SUFFIX))

    // All parameters are as Freemarker parameters
    paramValues.foreach { case (k, v) => freemarkerParams.put(k, v) }

    // Build typed parameters
    val typedArgs = if (parameterizedEnabled)
      paramValues.map { case (name, value) =>
        val declaredType = typeDeclarations.get(name)
        name -> ParamTypeParser.parseTypedValue(value, declaredType)
      }
    else Map.empty[String, Any]

    (freemarkerParams, typedArgs)
  }
}
