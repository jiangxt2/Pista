package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.model.{Metrics, QueryPlanMetrics}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
 * Query plan extractor
 *
 * from each phase plan extracted from QueryExecution and statistics node information. QueryExecution Extract stage plans and aggregate node information.
 *
 */
class QueryPlanExtractor(truncateLength: Int = 10000) extends Logging {

  /**
   * Extract query plan metrics
   *
   * @param qe          QueryExecution
   * @param appId       Application ID
   * @param executionId SQL Execution ID
   * @param sqlHash     SQL Hash Value
   * @return QueryPlanMetrics
   */
  def extract(qe: QueryExecution, appId: String, executionId: Long, sqlHash: String): QueryPlanMetrics = {
    // Extract plans from each phase
    val logicalPlan = truncate(qe.logical.toString)
    val optimizedPlan = truncate(qe.optimizedPlan.toString)
    val physicalPlan = truncate(qe.sparkPlan.toString)
    val executedPlan = truncate(qe.executedPlan.toString)

    // Count the number of nodes
    val logicalPlanNodeCount = countNodes(qe.logical)
    val physicalPlanNodeCount = countNodes(qe.sparkPlan)

    // Statistics plan depth and width
    val (planDepth, planWidth) = calculateDepthAndWidth(qe.sparkPlan)

    QueryPlanMetrics(
      appId = appId,
      executionId = executionId,
      sqlHash = sqlHash,
      logicalPlan = logicalPlan,
      optimizedPlan = optimizedPlan,
      physicalPlan = physicalPlan,
      executedPlan = executedPlan,
      logicalPlanNodeCount = logicalPlanNodeCount,
      physicalPlanNodeCount = physicalPlanNodeCount,
      planDepth = planDepth,
      planWidth = planWidth,
      collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
    )
  }

  /**
   * truncate string
   */
  private def truncate(str: String): String = {
    if (str.length > truncateLength) {
      str.substring(0, truncateLength) + "..."
    } else {
      str
    }
  }

  /**
   * Count the number of logical plan nodes.
   */
  private def countNodes(plan: LogicalPlan): Int = {
    1 + plan.children.map(countNodes).sum
  }

  /**
   * Count the number of physical plan nodes.
   */
  private def countNodes(plan: SparkPlan): Int = {
    1 + plan.children.map(countNodes).sum
  }

  /**
   * Compute plan depth and width
   */
  private def calculateDepthAndWidth(plan: SparkPlan): (Int, Int) = {
    def traverse(node: SparkPlan, depth: Int): (Int, Int) = {
      if (node.children.isEmpty) {
        (depth, 1)
      } else {
        val childResults = node.children.map(child => traverse(child, depth + 1))
        val maxDepth = childResults.map(_._1).max
        val totalWidth = childResults.map(_._2).sum
        (maxDepth, totalWidth)
      }
    }

    traverse(plan, 1)
  }
}
