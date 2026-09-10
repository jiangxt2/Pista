package com.pista.spark.sql.execution.processor

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * ProcessorManager Test
 *
 * Complete the ProcessorSuite coverage missing:
 * - loadProcessors: enabled/disabled switch, class name resolution, ClassNotFound exception
 * - executeChain: empty-list short-circuit
 * - extractConfigexclusion of enabled/classes key
 *
 * Attention: ProcessorSuite covers executeChain for chaining execution, error handling, and extractConfig fundamental functions.
 *
 */
class ProcessorManagerSuite extends AnyFunSuite with BeforeAndAfterEach {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("ProcessorManagerSuite")
    .getOrCreate()

  override def afterEach(): Unit = {
    // Reset processor configuration
    spark.conf.set("spark.pista.processor.enabled", "false")
    spark.conf.set("spark.pista.processor.classes", "")
  }

  // ==================== loadProcessors processing ====================

  test("loadProcessors disabled should return an empty list") {
    spark.conf.set("spark.pista.processor.enabled", "false")
    spark.conf.set("spark.pista.processor.classes",
      "com.pista.spark.sql.execution.processor.TestProcessor")

    val processors = ProcessorManager.loadProcessors(spark)
    assert(processors.isEmpty)
  }

  test("loadProcessors Enable but without a class name should return an empty list") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", "")

    val processors = ProcessorManager.loadProcessors(spark)
    assert(processors.isEmpty)
  }

  test("loadProcessors testProcessors should correctly load known processor classes") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes",
      "com.pista.spark.sql.execution.processor.TestProcessor")

    val processors = ProcessorManager.loadProcessors(spark)
    assert(processors.size == 1)
    assert(processors.head.name == "TestProcessor")
  }

  test("loadProcessors multiple processors should be loaded") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes",
      "com.pista.spark.sql.execution.processor.TestProcessor," +
        "com.pista.spark.sql.execution.processor.TestProcessor2")

    val processors = ProcessorManager.loadProcessors(spark)
    assert(processors.size == 2)
    assert(processors.map(_.name).toSet == Set("TestProcessor", "TestProcessor2"))
  }

  test("loadProcessors ClassNotFound should throw an exception") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes",
      "com.nonexistent.FakeProcessor")

    assertThrows[Exception] {
      ProcessorManager.loadProcessors(spark)
    }
  }

  // ==================== executeChain Test ====================

  test("executeChain Empty Processor List Should Directly Return Original DataFrame") {
    import spark.implicits._
    val df = Seq((1, "test")).toDF("id", "name")
    val context = ProcessContext(spark, Map.empty, "test")

    val result = ProcessorManager.executeChain(df, Seq.empty, context)

    // Should reference the same DataFrame
    assert(result eq df)
  }

  test("executeChain Single Processor Should Correctly Execute") {
    import spark.implicits._
    val df = Seq((1, "Alice", 25), (2, "Bob", 15)).toDF("id", "name", "age")
    val context = ProcessContext(spark, Map.empty, "test")

    val result = ProcessorManager.executeChain(
      df, Seq(new TestProcessor), context
    )

    assert(result.count() == 1) // TestProcessor filtering age < 18
    assert(result.columns.contains("processed"))
  }

  // ==================== extractConfig Test Supplement ====================

  test("extractConfig returns an empty Map without additional configuration") {
    // Clear existing configuration
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", "")

    val config = ProcessorManager.extractConfig(spark)
    // enabled and classes should be excluded
    assert(!config.contains("enabled"))
    assert(!config.contains("classes"))
  }

  test("extractConfig should extract custom configuration and exclude enabled and classes") {
    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", "com.example.Proc")
    spark.conf.set("spark.pista.processor.batchSize", "100")
    spark.conf.set("spark.pista.processor.timeout", "30s")

    val config = ProcessorManager.extractConfig(spark)

    assert(config("batchSize") == "100")
    assert(config("timeout") == "30s")
    assert(!config.contains("enabled"))
    assert(!config.contains("classes"))
  }

  // ==================== ProcessContext Testing ====================

  test("ProcessContext Default values should be correct") {
    val ctx = ProcessContext(spark, Map("k" -> "v"), "test-sql")

    assert(ctx.sparkSession eq spark)
    assert(ctx.config == Map("k" -> "v"))
    assert(ctx.sqlName == "test-sql")
    assert(!ctx.isStreaming)
    assert(ctx.batchId.isEmpty)
    assert(ctx.queryName.isEmpty)
  }

  test("ProcessContext Streaming Mode Parameters Should Be Correctly Passed") {
    val ctx = ProcessContext(
      sparkSession = spark,
      config = Map.empty,
      sqlName = "streaming-sql",
      isStreaming = true,
      batchId = Some(42L),
      queryName = Some("test-query")
    )

    assert(ctx.isStreaming)
    assert(ctx.batchId.contains(42L))
    assert(ctx.queryName.contains("test-query"))
  }
}
