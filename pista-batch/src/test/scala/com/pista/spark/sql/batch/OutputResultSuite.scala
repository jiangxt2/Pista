package com.pista.spark.sql.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util

/**
 * Test SELECT query output functionality.
 *
 */
class OutputResultSuite extends AnyFunSuite with BeforeAndAfterAll {

  val outputPath = "/tmp/spark-sql-submitter-test-output"

  override def beforeAll(): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .enableHiveSupport
      .getOrCreate

    spark.sparkContext.setLogLevel("INFO")

    // Clean output directory
    deleteDirectory(new File(outputPath))
  }

  override def afterAll(): Unit = {
    // Clean output directory
    deleteDirectory(new File(outputPath))

    // Within a single forked JVM, this module serially executes suite and restores shared session output configuration.
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.output.format", "console")
    spark.conf.set("spark.pista.output.mode", "overwrite")
    try spark.conf.unset("spark.pista.output.path") catch {
      case _: Exception =>
    }
    try spark.conf.unset("spark.pista.output.partitionBy") catch {
      case _: Exception =>
    }
  }

  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      val files = directory.listFiles()
      if (files != null) {
        files.foreach { file =>
          if (file.isDirectory) {
            deleteDirectory(file)
          } else {
            file.delete()
          }
        }
      }
      directory.delete()
    }
  }

  test("Output SELECT query results to local file system in Parquet format") {
    // Configure output parameters using SparkSession of SparkSQLSubmitter
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.path", s"file://$outputPath/parquet")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.format", "parquet")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.mode", "overwrite")

    // Prepare test SQL
    val sql =
      """
        |SELECT 'Alice' as name, 25 as age, 'Engineer' as job
        |UNION ALL
        |SELECT 'Bob' as name, 30 as age, 'Manager' as job
        |UNION ALL
        |SELECT 'Charlie' as name, 35 as age, 'Director' as job
        |""".stripMargin

    val params = new util.HashMap[String, String]()

    // execute SQL
    SparkSQLSubmitter.submitSQL(sql, params)

    // Verify that the output file exists
    val outputDir = new File(s"$outputPath/parquet")
    assert(outputDir.exists(), "Output directory should exist")
    assert(outputDir.listFiles().exists(_.getName.endsWith(".parquet")), "Expected a Parquet file")

    // Read and validate data
    val result = SparkSQLSubmitter.sparkSession.read.parquet(s"file://$outputPath/parquet")
    assert(result.count() == 3, "there should be 3 record")
    assert(result.columns.length == 3, "there should be 3 three fields")
  }

  test("Output only queries SELECT") {
    // Configure output parameters
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.path", s"file://$outputPath/ddl")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.format", "parquet")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.mode", "overwrite")

    // Prepare test SQL (DDL statements)
    val sql =
      """
        |CREATE TEMPORARY VIEW test_view AS
        |SELECT 'test' as value
        |""".stripMargin

    val params = new util.HashMap[String, String]()

    // execute SQL
    SparkSQLSubmitter.submitSQL(sql, params)

    // Ensure output directory does not exist (since it is not a SELECT query)
    val outputDir = new File(s"$outputPath/ddl")
    assert(!outputDir.exists(), "DDL statement should not produce an output file")
  }

  test("console format does not write to file") {
    // Console format outputs results to standard output and does not generate a file.
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.format", "console")

    // Prepare test SQL
    val sql = "SELECT 'test' as value"
    val params = new util.HashMap[String, String]()

    // execute SQL
    SparkSQLSubmitter.submitSQL(sql, params)

    // Ensure output directory does not exist (console format does not produce a file)
    val outputDir = new File(s"$outputPath/disabled")
    assert(!outputDir.exists(), "console format should not produce an output file")
  }

  test("WITH clause supported SELECT query") {
    // Configure output parameters
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.path", s"file://$outputPath/with_clause")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.format", "parquet")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.mode", "overwrite")

    // Prepare test SQL (using WITH clause)
    val sql =
      """
        |WITH temp_data AS (
        |  SELECT 1 as id, 'A' as name
        |  UNION ALL
        |  SELECT 2 as id, 'B' as name
        |)
        |SELECT * FROM temp_data
        |""".stripMargin

    val params = new util.HashMap[String, String]()

    // execute SQL
    SparkSQLSubmitter.submitSQL(sql, params)

    // Verify that the output file exists
    val outputDir = new File(s"$outputPath/with_clause")
    assert(outputDir.exists(), "Output directory should exist")

    // Read and validate data
    val result = SparkSQLSubmitter.sparkSession.read.parquet(s"file://$outputPath/with_clause")
    assert(result.count() == 2, "there should be 2 two records")
  }

  test("supports partitioned writes") {
    // Configure output parameters
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.path", s"file://$outputPath/partitioned")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.format", "parquet")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.mode", "overwrite")
    SparkSQLSubmitter.sparkSession.conf.set("spark.pista.output.partitionBy", "category")

    // Prepare test SQL
    val sql =
      """
        |SELECT 'item1' as name, 'A' as category
        |UNION ALL
        |SELECT 'item2' as name, 'B' as category
        |UNION ALL
        |SELECT 'item3' as name, 'A' as category
        |""".stripMargin

    val params = new util.HashMap[String, String]()

    // execute SQL
    SparkSQLSubmitter.submitSQL(sql, params)

    // Verify that the output directory exists and contains partition directories
    val outputDir = new File(s"$outputPath/partitioned")
    assert(outputDir.exists(), "Output directory should exist")
    assert(outputDir.listFiles().exists(_.getName.startsWith("category=")), "The output directory should contain a shard directory.")

    // Read and validate data
    val result = SparkSQLSubmitter.sparkSession.read.parquet(s"file://$outputPath/partitioned")
    assert(result.count() == 3, "there should be 3 record")
  }
}
