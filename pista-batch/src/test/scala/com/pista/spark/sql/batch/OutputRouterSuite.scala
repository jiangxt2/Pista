package com.pista.spark.sql.batch

import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.execution.datasources.writer.OutputConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * OutputRouter Routing Logic Test
 *
 * Covers output configuration extraction and routing for built-in, custom, and unsupported formats.
 */
class OutputRouterSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder.master("local[2]").enableHiveSupport.getOrCreate
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.pista.output.format", "console")
    spark.conf.set("spark.pista.output.mode", "overwrite")
    try spark.conf.unset("spark.pista.output.path") catch {
      case _: Exception =>
    }
    try spark.conf.unset("spark.pista.output.partitionBy") catch {
      case _: Exception =>
    }
  }

  override def afterAll(): Unit = {
    // SparkSession current fork JVM manage uniformly, cannot be shared across sessions during suite stop sharing SparkContext.
  }

  // ==================== extractOutputConfig ====================

  test("extractOutputConfig default value: console format, overwrite mode") {
    val conf = ConfigReader(spark)
    val config = OutputRouter.extractOutputConfig(conf)

    assert(config.format === "console")
    assert(config.mode === "overwrite")
    assert(config.path.isEmpty)
    assert(config.partitionBy.isEmpty)
  }

  test("extractOutputConfig Custom format and path") {
    spark.conf.set("spark.pista.output.format", "parquet")
    spark.conf.set("spark.pista.output.path", "/tmp/test-output")
    spark.conf.set("spark.pista.output.mode", "append")
    spark.conf.set("spark.pista.output.partitionBy", "dt,region")

    val conf = ConfigReader(spark)
    val config = OutputRouter.extractOutputConfig(conf)

    assert(config.format === "parquet")
    assert(config.path === Some("/tmp/test-output"))
    assert(config.mode === "append")
    assert(config.partitionBy === Seq("dt", "region"))

    // Clean
    spark.conf.set("spark.pista.output.format", "console")
    spark.conf.set("spark.pista.output.path", "")
    spark.conf.set("spark.pista.output.mode", "overwrite")
    spark.conf.unset("spark.pista.output.partitionBy")
  }

  // ==================== write routing ====================

  test("write console format: no exception thrown") {
    val df = spark.range(5).toDF("id")
    val config = OutputConfig(path = None, format = "console", mode = "overwrite",
      partitionBy = Seq.empty, options = Map.empty)
    // console format should successfully execute (show)
    OutputRouter.write(df, config)
  }

  test("write unknown format: throws writerNotFoundError") {
    val df = spark.range(5).toDF("id")
    val config = OutputConfig(path = None, format = "unknown_format", mode = "overwrite",
      partitionBy = Seq.empty, options = Map.empty)

    intercept[Exception] {
      OutputRouter.write(df, config)
    }
  }

  test("write parquet format missing path: throws writerValidationError") {
    val df = spark.range(5).toDF("id")
    val config = OutputConfig(path = None, format = "parquet", mode = "overwrite",
      partitionBy = Seq.empty, options = Map.empty)

    intercept[Exception] {
      OutputRouter.write(df, config)
    }
  }
}
