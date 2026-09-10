package com.pista.spark.sql.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.errors.ErrorPolicyResolver
import com.pista.spark.sql.execution.ExecutionReport
import com.pista.spark.sql.execution.processor.{ProcessContext, ProcessorManager}
import com.pista.spark.sql.metrics.processor.DataQualityMetricsProcessor
import com.pista.spark.util.{LogRedaction, SQLTemplateEngine}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.catalyst.plans.logical.{Command, V2WriteCommand}

/**
 * Executes split SQL statements with query classification, processors, output routing, error
 * policies, and post-action callbacks.
 *
 * Extracted from SparkSQLSubmitter, responsible for a single task.
 */
object SQLStatementExecutor extends Logging {

  private val SparkJobDescriptionKey = "spark.job.description"
  private final class ContinuedOutputFailure(error: Throwable)
    extends RuntimeException(LogRedaction.exceptionName(error))

  def execute(spark: SparkSession, processedSQL: String, typedArgs: Map[String, Any], conf: ConfigReader): Unit = {
    val outputConfig = OutputRouter.extractOutputConfig(conf)

    // Initialize error strategy and execution report
    val policyResolver = new ErrorPolicyResolver(spark)
    val sqlPolicy = policyResolver.sqlErrorPolicy
    val report = new ExecutionReport()

    // Load custom processor (only when enabled)
    val processors = ProcessorManager.loadProcessors(spark, conf)
    val processorConfig = if (processors.nonEmpty) Some(ProcessorManager.extractConfig(spark, conf)) else None

    // Data quality metrics collection automatic injection (enabled together with metrics.enabled)
    val metricsEnabled = conf.get(SubmitterConf.METRICS_ENABLED)
    val metricsProcessors = if (metricsEnabled) Seq(new DataQualityMetricsProcessor()) else Seq.empty
    // Remove a manually configured duplicate when metrics already inject the processor.
    val dqClassName = classOf[DataQualityMetricsProcessor].getName
    val dedupedProcessors = if (metricsEnabled) {
      val filtered = processors.filterNot(_.getClass.getName == dqClassName)
      if (filtered.size < processors.size)
        logWarning(
          "Ignoring the manually configured DataQualityMetricsProcessor because " +
            "metrics.enabled already injects it")
      filtered
    } else processors
    val allProcessors = metricsProcessors ++ dedupedProcessors

    val sqlStatements = SQLTemplateEngine.splitStatements(processedSQL).zipWithIndex

    for ((sqlContent, index) <- sqlStatements) {
      val startTime = System.currentTimeMillis()
      val previousJobDescription =
        spark.sparkContext.getLocalProperty(SparkJobDescriptionKey)
      val sqlFingerprint = LogRedaction.fingerprint(sqlContent)
      spark.sparkContext.setLocalProperty(
        SparkJobDescriptionKey,
        s"pista-sql-${index + 1}-$sqlFingerprint")
      var selectContext: Option[ProcessContext] = None
      // actionTriggered becomes true only after the output action completes.
      // If write fails before an Action is triggered and is caught by outputPolicy=continue, keep it as false,
      // Post-processed callback does not execute to avoid permanent blocking of observation.get.
      var actionTriggered = false

      /* SQL execution phase: exception strategy proceeds to sqlPolicy */
      val sqlOk = try {
        logInfo(
          s"Execute SQL [${index + 1}/${sqlStatements.length}], fingerprint=$sqlFingerprint")

        def executeSql() =
          if (typedArgs.nonEmpty) spark.sql(sqlContent, typedArgs)
          else spark.sql(sqlContent)

        var result = executeSql()

        if (isSelectQuery(result)) {
          // Create a separate context for each SQL, isolating postActionCallbacks and sqlName.
          val context = ProcessContext(
            sparkSession = spark,
            config = processorConfig.getOrElse(Map.empty),
            sqlName = s"sql_${index + 1}"
          )
          selectContext = Some(context)
          if (allProcessors.nonEmpty)
            result = ProcessorManager.executeChain(result, allProcessors, context)

          // outputPolicy handles it: set actionTriggered=true only upon successful write.
          try {
            OutputRouter.write(result, outputConfig)
            actionTriggered = true
          } catch {
            case e: Exception =>
              logError(
                s"Output write failed with ${LogRedaction.exceptionName(e)}",
                LogRedaction.sanitizedThrowable(e))
              if (policyResolver.outputErrorPolicy.shouldFailFast)
                throw LogRedaction.sanitizedThrowable(e)
              else {
                logWarning(
                  "Output error policy is continue-on-error; remaining statements will run, " +
                    "but the job outcome will be failed")
                throw new ContinuedOutputFailure(e)
              }
              // actionTriggered set to false, callback not executed
          }
        } else {
          // INSERT/DDL: Do not pass through the Processor pipeline
          // INSERT metrics are collected by the observation path; DDL has no rows to validate.
          val rows = result.collect()
          if (rows.nonEmpty)
            logInfo(s"Command completed with ${rows.length} result row(s)")
          else
            logInfo("Command completed (no result rows)")
        }

        val duration = System.currentTimeMillis() - startTime
        report.addSuccess(index + 1, sqlContent, duration)
        logInfo(s"SQL [${index + 1}] executed successfully in ${duration}ms")
        true
      } catch {
        case e: ContinuedOutputFailure =>
          val duration = System.currentTimeMillis() - startTime
          report.addFailure(index + 1, sqlContent, e, duration)
          false
        case e: Throwable =>
          val duration = System.currentTimeMillis() - startTime
          report.addFailure(index + 1, sqlContent, e, duration)
          logError(
            s"SQL [${index + 1}] failed with ${LogRedaction.exceptionName(e)}",
            LogRedaction.sanitizedThrowable(e))

          if (sqlPolicy.shouldFailFast) {
            logError("Error policy is fail-fast, stopping execution")
            throw LogRedaction.sanitizedThrowable(e)
          } else {
            logWarning(s"Error policy is continue-on-error, skipping failed SQL and continuing")
            false
          }
      } finally {
        spark.sparkContext.setLocalProperty(SparkJobDescriptionKey, previousJobDescription)
      }

      // ── Post-processing callback stage: triggered after SQL succeeds and Action completes, independent of processorErrorPolicy ──
      // This block is outside the SQL-policy catch so processor fail-fast remains independent.
      if (sqlOk && actionTriggered)
        selectContext.foreach { context =>
          if (context.postActionCallbacks.nonEmpty) {
            val processorPolicy = policyResolver.processorErrorPolicy
            context.postActionCallbacks.foreach { cb =>
              try cb()
              catch {
                case e: Throwable =>
                  logError(
                    s"[sql_${index + 1}] post-action callback failed with " +
                      LogRedaction.exceptionName(e),
                    LogRedaction.sanitizedThrowable(e))
                  if (processorPolicy.shouldFailFast)
                    throw LogRedaction.sanitizedThrowable(e)
                  else {
                    logWarning(
                      "Processor error policy is continue-on-error; skipping callback failure")
                    report.replaceWithFailure(
                      index + 1,
                      sqlContent,
                      e,
                      System.currentTimeMillis() - startTime)
                  }
              }
            }
          }
        }
    }

    // Output a report
    if (report.hasFailures) {
      logWarning(report.formatReport)
      throw PistaErrors.sqlExecutionFailedError(report.failureCount, report.totalCount)
    } else {
      logInfo(s"All ${report.totalCount} SQL statements executed successfully")
    }
  }

  // ========== Determine Query Type ==========

  private[batch] def isSelectQuery(df: DataFrame): Boolean = {
    try {
      // Use analyzed plan to determine (INSERT is parsed as DataWritingCommand/V2WriteCommand during the analyzed phase)
      val analyzedPlan = df.queryExecution.analyzed

      // Determine if it is an INSERT (V1 or V2)
      val isInsert = analyzedPlan match {
        case _: DataWritingCommand => true  // V1: INSERT INTO/OVERWRITE
        case _: V2WriteCommand => true      // V2: AppendData/OverwriteByExpression/OverwritePartitionsDynamic
        case _ => false
      }

      // Determine if it is another Command (DDL/DML)
      val isOtherCommand = !isInsert && analyzedPlan.isInstanceOf[Command]

      // Check for a valid schema
      val hasValidSchema = df.schema.nonEmpty

      // Three-Class Logic:
      // - INSERT → false (utilizing observe for collection, not passing through Processor)
      // - Other Command (DDL) → false (No Data, Not Applicable for Quality Check)
      // - Other (SELECT) → true (Proceed to Processor Chain)
      val isQuery = !isInsert && !isOtherCommand && hasValidSchema

      if (isInsert)
        logInfo(s"Detected INSERT statement (observe will collect metrics)")
      else if (isOtherCommand)
        logInfo(s"Detected DDL/Command statement (no metrics)")
      else
        logInfo(s"Detected SELECT query (processor chain will apply)")

      isQuery
    } catch {
      case e: Exception =>
        logWarning(
          s"Failed to determine query type with ${LogRedaction.exceptionName(e)}")
        false
    }
  }

}
