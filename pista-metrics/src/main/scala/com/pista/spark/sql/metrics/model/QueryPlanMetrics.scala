package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * metrics related to query plan analysis
 *
 * @param metricType           Metric type, fixed as "query_plan"
 * @param appId                Application ID
 * @param executionId          SQL Execution ID
 * @param sqlHash              SQL Hash Value
 * @param logicalPlan          Logical plan (potentially truncated)
 * @param optimizedPlan        Optimized logical plan (may be truncated)
 * @param physicalPlan Physical Plan (potentially truncated)
 * @param executedPlan         Executed Plan (potentially truncated)
 * @param logicalPlanNodeCount Logical plan node count
 * @param physicalPlanNodeCount Physical plan node count
 * @param planDepth            Plan Depth
 * @param planWidth            Width of the plan
 * @param collectTimestamp          Collection Timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
case class QueryPlanMetrics(
  metricType: String = "query_plan",
  appId: String,
  executionId: Long,
  sqlHash: String,

  // Plan text (may be truncated)
  logicalPlan: String,
  optimizedPlan: String,
  physicalPlan: String,
  executedPlan: String,

  // Plan statistics
  logicalPlanNodeCount: Int,
  physicalPlanNodeCount: Int,
  planDepth: Int,
  planWidth: Int,

  collectTimestamp: String
) extends Metrics

object QueryPlanMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: QueryPlanMetrics): String = mapper.writeValueAsString(metrics)
}
