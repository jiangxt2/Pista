package com.pista.spark.sql.metrics.processor

import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.execution.processor.{DataProcessor, ProcessContext}
import com.pista.spark.sql.metrics.collector.DataQualityExtractor
import com.pista.spark.sql.metrics.MetricsManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.{DataFrame, Observation}

/**
 * Data quality metrics processor collector
 *
 * Observe data quality metrics without blocking task execution by collecting data.
 * The collected results are written to the metric file by MetricsManager.
 *
 */
class DataQualityMetricsProcessor extends DataProcessor with Logging {
  private val NumericPercentilesEnabled = true
  private val NumericPercentilePoints: Seq[Double] = Seq(0.5, 0.95, 0.99)

  override def name: String = "DataQualityMetrics"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    val sqlIndex  = context.sqlName
    val tableName = resolveSelectTableName(df, sqlIndex)
    val executionId = df.queryExecution.id
    val reader = ConfigReader(context.sparkSession)
    val maxColumns = reader.get(SubmitterConf.METRICS_DQ_MAX_COLUMNS)
    val extractor = new DataQualityExtractor()
    val (observeExprs, meta) = extractor.buildObserveExprs(
      df          = df,
      appId       = context.sparkSession.sparkContext.applicationId,
      executionId = executionId,
      tableName   = tableName,
      sqlIndex    = Some(sqlIndex),
      maxColumns  = maxColumns,
      numericPercentilesEnabled = NumericPercentilesEnabled,
      numericPercentilePoints = NumericPercentilePoints
    )
    val observation = Observation(s"dq_${context.sqlName}")
    val observed    = df.observe(observation, observeExprs.head, observeExprs.tail: _*)

    context.postActionCallbacks += { () =>
      val metrics = extractor.parseFromObservation(observation.get, meta)
      MetricsManager.get.foreach(_.enqueue(metrics))
      logInfo(s"[$name] Data quality metric collection is complete: totalRecords=${metrics.totalRecords}")
    }
    observed
  }

  /**
   * SELECT path_table_name_inference:
   * 1. Prioritize extracting parsed V2/V1 relationship names (consistent with the INSERT path)
   * 2. If no relation source (such as inline subquery), rollback to subquery alias.
   * 3. For unrecognized cases, use sqlIndex as a fallback.
   */
  private[processor] def resolveSelectTableName(df: DataFrame, fallback: String): String = {
    val analyzed = df.queryExecution.analyzed

    val v2Names = analyzed.collect {
      case rel: DataSourceV2Relation =>
        val ident = rel.identifier.map(_.toString).filter(_.nonEmpty)
        (rel.catalog, ident) match {
          case (Some(catalog), Some(identifier)) => s"${catalog.name()}.$identifier"
          case (None, Some(identifier)) => identifier
          case _ => Option(rel.name).filter(_.nonEmpty).getOrElse(rel.getClass.getSimpleName)
        }
    }

    val v1Names = analyzed.collect {
      case rel: LogicalRelation =>
        rel.catalogTable.map(_.identifier.unquotedString)
    }.flatten

    val relationNames = (v2Names ++ v1Names).filter(_.nonEmpty).distinct
    if (relationNames.nonEmpty) {
      relationNames.mkString(",")
    } else {
      analyzed
        .collectFirst { case alias: SubqueryAlias => alias.identifier.name }
        .filter(_.nonEmpty)
        .getOrElse(fallback)
    }
  }
}
