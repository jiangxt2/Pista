package com.pista.spark.sql.metrics.processor

import com.pista.spark.sql.execution.processor.ProcessContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * Data quality metrics processor test collector
 *
 * process() returns a DataFrame with an observer attached, without triggering an Action immediately.
 * Test mode:
 *   1. processor.process(df, context) → obtained observed df
 *   2. trigger occurs Action(observed.count())→ observation Collection complete
 *   3. manually executing context.postActionCallbacks → validate that metrics collection is normal
 *
 */
class DataQualityMetricsProcessorSuite extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("DataQualityMetricsProcessorSuite")
    .getOrCreate()

  private val processor = new DataQualityMetricsProcessor

  /** Trigger an Action and execute all post-processing callbacks auxiliary method */
  private def triggerAndRunCallbacks(
    observed: org.apache.spark.sql.DataFrame,
    context: ProcessContext
  ): Unit = {
    observed.count()
    context.postActionCallbacks.foreach(_())
  }

  test("Only normal data completes the metric collection without throwing an exception.") {
    import spark.implicits._
    val df      = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val context = ProcessContext(spark, Map.empty, "test_sql")

    val observed = processor.process(df, context)
    triggerAndRunCallbacks(observed, context)
  }

  test("process() returns the same number of rows in the DataFrame as the original DataFrame") {
    import spark.implicits._
    val df      = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val context = ProcessContext(spark, Map.empty, "test_sql")

    val observed = processor.process(df, context)
    assert(observed.count() == 3)
    context.postActionCallbacks.foreach(_())
  }

  test("Empty value data should be collected normally without throwing an exception.") {
    import spark.implicits._
    val df      = Seq(Tuple1(Some("a")), Tuple1(None), Tuple1(None)).toDF("name")
    val context = ProcessContext(spark, Map.empty, "test_sql")

    val observed = processor.process(df, context)
    triggerAndRunCallbacks(observed, context)
  }

  test("the observe path collects min/max/avg columnStats for numeric columns") {
    import spark.implicits._
    val df      = Seq((10, "a"), (20, "b"), (30, "c")).toDF("value", "name")
    val context = ProcessContext(spark, Map.empty, "test_sql")

    val observed = processor.process(df, context)
    triggerAndRunCallbacks(observed, context)
  }

  test("two independent context the two independent contexts postActionCallbacks do not interfere with each other. postActionCallbacks independent") {
    import spark.implicits._
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    val df2 = Seq((2, "Bob")).toDF("id", "name")

    val ctx1 = ProcessContext(spark, Map.empty, "sql_1")
    val ctx2 = ProcessContext(spark, Map.empty, "sql_2")

    val obs1 = processor.process(df1, ctx1)
    val obs2 = processor.process(df2, ctx2)

    assert(ctx1.postActionCallbacks.size == 1)
    assert(ctx2.postActionCallbacks.size == 1)

    obs1.count()
    ctx1.postActionCallbacks.foreach(_())

    obs2.count()
    ctx2.postActionCallbacks.foreach(_())
  }

  test("SELECT temporary view to extract the actual tableName rather than sqlIndex") {
    import spark.implicits._
    Seq((1, "a"), (2, "b")).toDF("id", "name").createOrReplaceTempView("dq_src_table")
    val df = spark.sql("SELECT id, name FROM dq_src_table")

    val tableName = processor.resolveSelectTableName(df, "sql_1")
    assert(tableName == "dq_src_table")
  }

  test("No Relation Source Backquote Subquery Alias as tableName") {
    val df = spark.sql("SELECT id FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) orders")

    val tableName = processor.resolveSelectTableName(df, "sql_1")
    assert(tableName == "orders")
  }
}
