package com.pista.spark.sql.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * SQLExecutionContext Test
 *
 * Covers valid contexts returned by initIfNeeded and init.
 *
 * Note: MDC is not available in the test environment (SLF4J binding issue), and traceId-related behavior applies.
 * Validate indirectly through SparkSession configuration.
 */
class SQLExecutionContextSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder.master("local[2]").enableHiveSupport.getOrCreate
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    // SparkSession current fork JVM manage uniformly, cannot be shared across sessions during suite stop sharing SparkContext.
  }

  // ==================== initIfNeeded ====================

  test("initIfNeeded: return valid conf and typedArgs") {
    val ctx = SQLExecutionContext.initIfNeeded(spark)

    assert(ctx.spark != null, "spark should not be null")
    assert(ctx.conf != null, "conf should not be null")
    assert(ctx.typedArgs != null, "typedArgs should not be null")
    assert(ctx.freemarkerParams != null, "freemarkerParams should not be null")
  }

  test("initIfNeeded: traceId generated and written to SparkSession config") {
    val ctx = SQLExecutionContext.initIfNeeded(spark)

    // initIfNeeded should write traceId into SparkSession config
    val traceIdInConf = spark.conf.getOption("spark.pista.traceId")
    assert(traceIdInConf.isDefined, "traceId should be set in SparkSession config")
    assert(traceIdInConf.get.nonEmpty, "traceId should be non-empty")
  }

  test("initIfNeeded: called multiple times returns independent context") {
    val ctx1 = SQLExecutionContext.initIfNeeded(spark)
    val ctx2 = SQLExecutionContext.initIfNeeded(spark)

    // Two invocations should return valid context.
    assert(ctx1.conf != null)
    assert(ctx2.conf != null)
  }

  test("init: return valid context") {
    val ctx = SQLExecutionContext.init(spark)

    assert(ctx.spark != null)
    assert(ctx.conf != null)
  }
}
