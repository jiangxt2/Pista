package com.pista.spark.sql.batch

import com.pista.spark.errors.{PistaExecutionException, PistaSQLFileException}
import com.pista.spark.sql.functions.PistaFunctionInstaller
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util

/**
 * SparkSQLSubmitter core process test
 *
 * Covers SQL execution, templates, multiple statements, output, error policies, catalog registration, and parameters.
 *
 */
class SparkSQLSubmitterSuite extends AnyFunSuite with BeforeAndAfterAll {

  val outputPath = "/tmp/spark-sql-submitter-suite-output"
  val sqlFilePath = "/tmp/spark-sql-submitter-suite-test.sql"

  override def beforeAll(): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .enableHiveSupport
      .getOrCreate

    spark.sparkContext.setLogLevel("INFO")

    PistaFunctionInstaller.install(SparkSQLSubmitter.sparkSession)
    deleteDirectory(new File(outputPath))
  }

  override def afterAll(): Unit = {
    resetOutputConfig()
    deleteDirectory(new File(outputPath))
    new File(sqlFilePath).delete()
  }

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { f =>
        if (f.isDirectory) deleteDirectory(f) else f.delete()
      })
      dir.delete()
    }

  private def resetOutputConfig(): Unit = {
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.output.format", "console")
    spark.conf.set("spark.pista.output.path", "")
    spark.conf.set("spark.pista.output.errorPolicy", "fail-fast")
    spark.conf.set("spark.pista.processor.enabled", "false")
    spark.conf.set("spark.pista.processor.classes", "")
    spark.conf.set("spark.pista.processor.errorPolicy", "fail-fast")
    spark.conf.set("spark.pista.metrics.enabled", "false")
  }

  // ==================== Basic SQL Execution ====================

  test("submitSQL should execute a simple SELECT query") {
    resetOutputConfig()
    val sql = "SELECT 1 as id, 'hello' as msg"
    val params = new util.HashMap[String, String]()

    // No exception thrown indicates success.
    SparkSQLSubmitter.submitSQL(sql, params)
  }

  test("submitSQL should execute multi-statement SQL") {
    resetOutputConfig()
    val sql =
      """
        |CREATE TEMPORARY VIEW multi_stmt_test AS SELECT 1 as id, 'Alice' as name;
        |SELECT * FROM multi_stmt_test
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    // Verify that the view has been created
    val result = SparkSQLSubmitter.sparkSession.sql("SELECT * FROM multi_stmt_test")
    assert(result.count() == 1)
    assert(result.columns.contains("name"))
  }

  test("submitSQL should support DDL statement represents DDL statements.") {
    resetOutputConfig()
    val sql =
      """
        |CREATE TEMPORARY VIEW ddl_test AS
        |SELECT 'test' as value, 42 as number
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    val result = SparkSQLSubmitter.sparkSession.sql("SELECT * FROM ddl_test")
    assert(result.count() == 1)
  }

  // ==================== Freemarker Template Rendering ====================

  test("submitSQL should support Freemarker template parameter substitution") {
    resetOutputConfig()
    val sql = "SELECT '${name}' as name, ${age} as age"
    val params = new util.HashMap[String, String]()
    params.put("name", "Alice")
    params.put("age", "25")

    SparkSQLSubmitter.submitSQL(sql, params)
  }

  test("submitSQL should support Freemarker condition template") {
    resetOutputConfig()
    val sql =
      """
        |SELECT
        |  'test' as value
        |  <#if filter??>
        |  ,'${filter}' as filter_value
        |  </#if>
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    params.put("filter", "active")

    SparkSQLSubmitter.submitSQL(sql, params)
  }

  // ==================== SELECT OUTPUT QUERY RESULTS ====================

  test("submitSQL should output the results of SELECT statements in JSON format") {
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.output.path", s"file://$outputPath/json_output")
    spark.conf.set("spark.pista.output.format", "json")
    spark.conf.set("spark.pista.output.mode", "overwrite")

    val sql =
      """
        |SELECT 'Alice' as name, 25 as age
        |UNION ALL
        |SELECT 'Bob' as name, 30 as age
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    val outputDir = new File(s"$outputPath/json_output")
    assert(outputDir.exists(), "JSON output directory should exist")

    val result = spark.read.json(s"file://$outputPath/json_output")
    assert(result.count() == 2)

    resetOutputConfig()
  }

  test("submitSQL should output the results of SELECT statements to CSV format") {
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.output.path", s"file://$outputPath/csv_output")
    spark.conf.set("spark.pista.output.format", "csv")
    spark.conf.set("spark.pista.output.mode", "overwrite")

    val sql = "SELECT 1 as id, 'test' as value"
    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    val outputDir = new File(s"$outputPath/csv_output")
    assert(outputDir.exists(), "CSV output directory should exist")

    resetOutputConfig()
  }

  // ==================== Error Strategy ====================

  test("fail-fast strategy should throw an exception when SQL fails SQL throw an exception when failure occurs") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.errorPolicy", "fail-fast")

    val sql = "SELECT * FROM non_existent_table_xyz"
    val params = new util.HashMap[String, String]()

    assertThrows[Exception] {
      SparkSQLSubmitter.submitSQL(sql, params)
    }
  }

  test("continue-on-error strategy should continue execution on SQL failure") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.errorPolicy", "continue-on-error")

    // First SQL failed, second one should continue to execute.
    val sql =
      """
        |SELECT * FROM non_existent_table_abc;
        |CREATE TEMPORARY VIEW continue_test AS SELECT 'survived' as status
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    val error = intercept[PistaExecutionException] {
      SparkSQLSubmitter.submitSQL(sql, params)
    }
    assert(error.getErrorClass === "PISTA_SQL_EXECUTION_FAILED")

    val result = spark.sql("SELECT * FROM continue_test")
    assert(result.first().getString(0) == "survived")

    // overwrite default configuration
    spark.conf.set("spark.pista.errorPolicy", "fail-fast")
  }

  test("SELECT Action is successfully triggered Action should execute postActionCallbacks") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    CallbackTrackingProcessor.reset()

    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", classOf[CallbackTrackingProcessor].getName)

    val sql = "SELECT 1 as id"
    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    assert(CallbackTrackingProcessor.processCount.get() == 1)
    assert(CallbackTrackingProcessor.callbackCount.get() == 1)

    resetOutputConfig()
  }

  test("output failure under continue-on-error does not execute postActionCallbacks") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    CallbackTrackingProcessor.reset()

    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", classOf[CallbackTrackingProcessor].getName)

    spark.conf.set("spark.pista.output.path", "") // Trigger write parameter validation failure
    spark.conf.set("spark.pista.output.format", "parquet")
    spark.conf.set("spark.pista.output.mode", "overwrite")
    spark.conf.set("spark.pista.output.errorPolicy", "continue-on-error")

    val sql = "SELECT 1 as id"
    val params = new util.HashMap[String, String]()
    val error = intercept[PistaExecutionException] {
      SparkSQLSubmitter.submitSQL(sql, params)
    }
    assert(error.getErrorClass === "PISTA_SQL_EXECUTION_FAILED")

    assert(CallbackTrackingProcessor.processCount.get() == 1)
    assert(CallbackTrackingProcessor.callbackCount.get() == 0)

    resetOutputConfig()
  }

  // ==================== Catalyst Catalog Verification ====================

  test("Catalog Installation should allow calling the Network function") {
    resetOutputConfig()
    val sql = "SELECT pista_ip_family('127.0.0.1') AS result"
    val params = new util.HashMap[String, String]()

    SparkSQLSubmitter.submitSQL(sql, params)

    val result = SparkSQLSubmitter.sparkSession.sql(sql)
    assert(result.first().getInt(0) === 4)
  }

  test("Catalog Installation Should Allow Calling JSON Function") {
    resetOutputConfig()
    val sql = "SELECT pista_json_is_valid('{\"valid\":true}') AS result"
    val params = new util.HashMap[String, String]()

    SparkSQLSubmitter.submitSQL(sql, params)

    val result = SparkSQLSubmitter.sparkSession.sql(sql)
    assert(result.first().getBoolean(0) === true)
  }

  // ==================== Parameterized Query ====================

  test("Parameterized queries should correctly pass typed parameters.") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.parameterized.enabled", "true")

    val sql = "SELECT :name as name, :age as age"
    val params = new util.HashMap[String, String]()
    val typedArgs: Map[String, Any] = Map("name" -> "Alice", "age" -> 25)

    SparkSQLSubmitter.submitSQL(sql, params, typedArgs)
  }

  test("empty typedArgs should take the normal SQL execution path") {
    resetOutputConfig()
    val sql = "SELECT 'normal' as mode"
    val params = new util.HashMap[String, String]()

    // typedArgs is null should not throw an exception
    SparkSQLSubmitter.submitSQL(sql, params, Map.empty)
  }

  // ==================== SQL File Read ====================

  test("submitSQL should correctly handle SQL file content") {
    resetOutputConfig()
    // Create a temporary SQL file.
    val writer = new PrintWriter(new File(sqlFilePath))
    try {
      writer.write("SELECT 'from_file' as source, 100 as value")
    } finally {
      writer.close()
    }

    // Read and execute via SQLTemplateEngine
    import com.pista.spark.util.SQLTemplateEngine
    val sqlContent = SQLTemplateEngine.readSQLFile(sqlFilePath)
    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sqlContent, params)
  }

  test("main should execute the entry for a single file via sqlFile_") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    val writer = new PrintWriter(new File(sqlFilePath))
    try {
      writer.write("CREATE TEMPORARY VIEW main_entry_test AS SELECT 7 AS id")
    } finally {
      writer.close()
    }

    spark.conf.set("spark.pista.sqlFile_", sqlFilePath)
    try {
      SparkSQLSubmitter.main(Array.empty)
      assert(spark.table("main_entry_test").first().getInt(0) == 7)
    } finally {
      spark.conf.unset("spark.pista.sqlFile_")
      spark.sql("DROP VIEW IF EXISTS main_entry_test")
    }
  }

  test("main should resolve a SQL file distributed through SparkFiles") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    val source = Files.createTempFile("pista-spark-files-", ".sql")
    val distributedFileName = source.getFileName.toString
    Files.write(
      source,
      "CREATE TEMPORARY VIEW spark_files_entry_test AS SELECT 11 AS id"
        .getBytes(StandardCharsets.UTF_8))
    spark.sparkContext.addFile(source.toString)

    spark.conf.set("spark.pista.sqlFile_", distributedFileName)
    try {
      SparkSQLSubmitter.main(Array.empty)
      assert(spark.table("spark_files_entry_test").first().getInt(0) == 11)
    } finally {
      spark.conf.unset("spark.pista.sqlFile_")
      spark.sql("DROP VIEW IF EXISTS spark_files_entry_test")
      Files.deleteIfExists(source)
    }
  }

  test("distributed SQL file resolution should reject missing and non-file paths") {
    val missingName = s"missing-${java.util.UUID.randomUUID()}.sql"
    val missingError = intercept[PistaSQLFileException] {
      SparkSQLSubmitter.resolveSQLFilePath(missingName)
    }
    assert(missingError.getErrorClass == "PISTA_INVALID_SQL_FILE")

    val directoryError = intercept[PistaSQLFileException] {
      SparkSQLSubmitter.resolveSQLFilePath(".")
    }
    assert(directoryError.getErrorClass == "PISTA_INVALID_SQL_FILE")

    val unresolvedPathError = intercept[PistaSQLFileException] {
      SparkSQLSubmitter.resolveSQLFilePath("missing/query.sql")
    }
    assert(unresolvedPathError.getErrorClass == "PISTA_INVALID_SQL_FILE")
  }

  // ==================== Complex SQL Scenarios ====================

  test("submitSQL should support CTE (WITH clause) Query occurs.") {
    resetOutputConfig()
    val sql =
      """
        |WITH base AS (
        |  SELECT 1 as id, 'A' as category
        |  UNION ALL
        |  SELECT 2 as id, 'B' as category
        |  UNION ALL
        |  SELECT 3 as id, 'A' as category
        |)
        |SELECT category, count(*) as cnt FROM base GROUP BY category
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)
  }

  test("submitSQL should support subqueries") {
    resetOutputConfig()
    val sql =
      """
        |SELECT * FROM (
        |  SELECT 1 as id, 'test' as name
        |  UNION ALL
        |  SELECT 2 as id, 'test2' as name
        |) t WHERE t.id > 1
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)
  }

  test("submitSQL should support window functions") {
    resetOutputConfig()
    val sql =
      """
        |SELECT
        |  id,
        |  name,
        |  ROW_NUMBER() OVER (ORDER BY id) as rn
        |FROM (
        |  SELECT 1 as id, 'Alice' as name
        |  UNION ALL
        |  SELECT 2 as id, 'Bob' as name
        |) t
        |""".stripMargin

    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)
  }

  test("metrics.enabled=true Metrics are collected normally without exceptions.") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.metrics.enabled", "true")

    val sql    = "SELECT 1 as id, 'Alice' as name"
    val params = new util.HashMap[String, String]()
    try SparkSQLSubmitter.submitSQL(sql, params)
    finally resetOutputConfig()
  }

  test("multiple SELECT SQL row SQL trigger independently process and in each SQL statement, the process and callback are triggered independently. callback") {
    resetOutputConfig()
    val spark = SparkSQLSubmitter.sparkSession
    CallbackTrackingProcessor.reset()

    spark.conf.set("spark.pista.processor.enabled", "true")
    spark.conf.set("spark.pista.processor.classes", classOf[CallbackTrackingProcessor].getName)

    val sql    = "SELECT 1 as id;\nSELECT 2 as id"
    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    // Two separate SELECT statements operate independently, triggering a process and a callback each.
    assert(CallbackTrackingProcessor.processCount.get() == 2)
    assert(CallbackTrackingProcessor.callbackCount.get() == 2)

    resetOutputConfig()
  }

  // ==================== isSelectQuery Indirect Testing ====================

  test("DDL The statement should not trigger output write.") {
    val spark = SparkSQLSubmitter.sparkSession
    spark.conf.set("spark.pista.output.path", s"file://$outputPath/ddl_no_output")
    spark.conf.set("spark.pista.output.format", "parquet")
    spark.conf.set("spark.pista.output.mode", "overwrite")

    val sql = "CREATE TEMPORARY VIEW no_output_test AS SELECT 1 as id"
    val params = new util.HashMap[String, String]()
    SparkSQLSubmitter.submitSQL(sql, params)

    val outputDir = new File(s"$outputPath/ddl_no_output")
    assert(!outputDir.exists(), "DDL statement should not produce an output file")

    resetOutputConfig()
  }
}
