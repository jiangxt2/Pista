package com.pista.spark.sql.execution.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Data processor test
 *
 */
class ProcessorSuite extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("ProcessorTest")
    .getOrCreate()

  test("custom processor should correctly handle data") {
    import spark.implicits._

    val df = Seq((1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 17))
      .toDF("id", "name", "age")

    val processor = new TestProcessor
    val context = ProcessContext(spark, Map.empty, "test")

    val result = processor.process(df, context)

    assert(result.count() == 2) // filter out age < 18
    assert(result.columns.contains("processed"))
  }

  test("processors chain should execute in order") {
    import spark.implicits._

    val df = Seq((1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 17))
      .toDF("id", "name", "age")

    val processor1 = new TestProcessor
    val processor2 = new TestProcessor2
    val context = ProcessContext(spark, Map.empty, "test")

    val result = ProcessorManager.executeChain(
      df,
      Seq(processor1, processor2),
      context
    )

    assert(result.count() == 2)
    assert(result.columns.contains("processed"))
    assert(result.columns.contains("processed2"))
  }

  test("A processor failure should not interrupt the process.") {
    import spark.implicits._

    val df = Seq((1, "Alice", 25), (2, "Bob", 30)).toDF("id", "name", "age")

    val processor1 = new TestProcessor
    val failingProcessor = new FailingProcessor
    val processor2 = new TestProcessor2
    val context = ProcessContext(spark, Map.empty, "test")

    // Configure the processor error strategy to continue-on-error.
    spark.conf.set("spark.pista.processor.errorPolicy", "continue-on-error")

    // Even if the processor fails internally, the process should continue.
    val result = ProcessorManager.executeChain(
      df,
      Seq(processor1, failingProcessor, processor2),
      context
    )

    // Failed processors should return the original DataFrame (i.e., the output of processor1).
    // processor1 adds "processed"; processor2 adds "processed2".
    assert(result.columns.contains("processed"))
    assert(result.columns.contains("processed2"))
  }

  test("ProcessorManager should correctly extract configuration") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", "com.example.CustomProcessor")
    spark.conf.set("spark.pista.processor.param1", "value1")
    spark.conf.set("spark.pista.processor.param2", "value2")

    val config = ProcessorManager.extractConfig(spark)

    assert(config.contains("param1"))
    assert(config("param1") == "value1")
    assert(config.contains("param2"))
    assert(config("param2") == "value2")
    assert(!config.contains("enabled"))
    assert(!config.contains("classes"))
  }

  test("UserTagProcessor emits consistent English labels") {
    import spark.implicits._

    val input = Seq(
      (1, 30000.0, 100000.0, 350),
      (2, 10000.0, 100000.0, 320),
      (3, 1000.0, 0.0, 30)
    ).toDF("id", "spending", "income", "active_days")

    val rows = new UserTagProcessor()
      .process(input, ProcessContext(spark, Map.empty, "user-tags"))
      .select("id", "user_level", "activity_level", "spending_power", "user_segment")
      .collect()
      .map(row => row.getInt(0) -> row.toSeq.drop(1).map(_.toString))
      .toMap

    assert(rows(1) == Seq("very_high_spending", "high_activity", "high", "high_value_user"))
    assert(rows(2) == Seq("moderate_spending", "high_activity", "moderate", "growth_user"))
    assert(rows(3) == Seq("low_spending", "low_activity", "no_income", "churn_risk"))
  }
}

// Test processor
class TestProcessor extends DataProcessor {
  override def name: String = "TestProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    import org.apache.spark.sql.functions._
    df.filter(col("age") >= 18)
      .withColumn("processed", lit(true))
  }
}

class TestProcessor2 extends DataProcessor {
  override def name: String = "TestProcessor2"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    import org.apache.spark.sql.functions._
    df.withColumn("processed2", lit(true))
  }
}

class FailingProcessor extends DataProcessor {
  override def name: String = "FailingProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    throw new RuntimeException("Intentional failure for testing")
  }
}
