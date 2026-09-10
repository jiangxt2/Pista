package com.pista.spark.sql.metrics.collector

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * Query Plan Extractor Testing
 *
 * Coverage: plan extraction, node counting, depth/shrink, extraction of optimization rules based on metrics calculation
 *
 */
class QueryPlanExtractorSuite extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("QueryPlanExtractorSuite")
    .getOrCreate()

  test("extract should extract plans from all stages") {
    import spark.implicits._
    val df = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    df.createOrReplaceTempView("qp_test")

    val result = spark.sql("SELECT * FROM qp_test WHERE id > 1")
    result.collect()

    val extractor = new QueryPlanExtractor()
    val metrics = extractor.extract(result.queryExecution, "test-app", 1L, "hash1")

    assert(metrics.appId == "test-app")
    assert(metrics.executionId == 1L)
    assert(metrics.logicalPlan.nonEmpty)
    assert(metrics.optimizedPlan.nonEmpty)
    assert(metrics.physicalPlan.nonEmpty)
    assert(metrics.executedPlan.nonEmpty)
    assert(metrics.metricType == "query_plan")
  }

  test("extract should correctly count the number of nodes") {
    import spark.implicits._
    val df = Seq((1, "a")).toDF("id", "name")
    df.createOrReplaceTempView("qp_node_test")

    val result = spark.sql("SELECT * FROM qp_node_test")
    result.collect()

    val extractor = new QueryPlanExtractor()
    val metrics = extractor.extract(result.queryExecution, "test-app", 1L, "hash")

    assert(metrics.logicalPlanNodeCount >= 1)
    assert(metrics.physicalPlanNodeCount >= 1)
    assert(metrics.planDepth >= 1)
    assert(metrics.planWidth >= 1)
  }

  test("extract Join more nodes should be present in the join query") {
    import spark.implicits._
    Seq((1, "a"), (2, "b")).toDF("id", "name").createOrReplaceTempView("qp_left")
    Seq((1, 100), (2, 200)).toDF("id", "score").createOrReplaceTempView("qp_right")

    val result = spark.sql("SELECT l.name, r.score FROM qp_left l JOIN qp_right r ON l.id = r.id")
    result.collect()

    val extractor = new QueryPlanExtractor()
    val metrics = extractor.extract(result.queryExecution, "test-app", 1L, "hash")

    // Join queries should have more nodes.
    assert(metrics.logicalPlanNodeCount >= 3)
    assert(metrics.physicalPlanNodeCount >= 3)
  }

  test("truncateLength should limit the length of the plan string") {
    import spark.implicits._
    val df = Seq((1, "a")).toDF("id", "name")
    df.createOrReplaceTempView("qp_truncate_test")

    val result = spark.sql("SELECT * FROM qp_truncate_test")
    result.collect()

    val extractor = new QueryPlanExtractor(truncateLength = 50)
    val metrics = extractor.extract(result.queryExecution, "test-app", 1L, "hash")

    // If the original plan exceeds 50 characters, it should be truncated and ended with ...
    if (result.queryExecution.logical.toString.length > 50) {
      assert(metrics.logicalPlan.length <= 53) // 50 + "..."
      assert(metrics.logicalPlan.endsWith("..."))
    }
  }

}
