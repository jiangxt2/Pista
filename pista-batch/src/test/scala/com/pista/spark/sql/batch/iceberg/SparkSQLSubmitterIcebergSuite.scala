package com.pista.spark.sql.batch.iceberg

import com.pista.spark.sql.batch.SparkSQLSubmitter
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util

/**
 * Native Spark SQL and Iceberg Catalog integration tests.
 *
 * These tests deliberately use Spark SQL and Iceberg's SparkCatalog directly. They do not use a
 * Pista Iceberg writer, Pista Iceberg configuration, or Pista snapshot metadata injection.
 */
class SparkSQLSubmitterIcebergSuite extends AnyFunSuite with BeforeAndAfterAll {

  private val warehousePath = "/tmp/spark-sql-submitter-iceberg-suite-warehouse"
  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    deleteDirectory(new File(warehousePath))

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQLSubmitterIcebergSuite")
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.local.type", "hadoop")
    spark.conf.set("spark.sql.catalog.local.warehouse", warehousePath)
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db_iceberg_test")
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.sql("DROP NAMESPACE IF EXISTS local.db_iceberg_test CASCADE")
    finally deleteDirectory(new File(warehousePath))
  }

  private def table(name: String): String = s"local.db_iceberg_test.$name"

  private def dropTable(name: String): Unit =
    spark.sql(s"DROP TABLE IF EXISTS ${table(name)}")

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { file =>
        if (file.isDirectory) deleteDirectory(file) else file.delete()
      })
      dir.delete()
    }

  test("SparkSQLSubmitter executes native Iceberg CTAS") {
    val target = table("native_ctas")
    try {
      SparkSQLSubmitter.submitSQLWithSession(
        spark,
        s"CREATE TABLE $target USING iceberg AS SELECT 1 AS id, 'Alice' AS name",
        new util.HashMap[String, String]())

      assert(spark.table(target).collect().length == 1)
      assert(spark.table(target).columns.toSeq == Seq("id", "name"))
    } finally dropTable("native_ctas")
  }

  test("SparkSQLSubmitter executes native Iceberg INSERT INTO") {
    val target = table("native_insert")
    try {
      spark.sql(s"CREATE TABLE $target (id INT, name STRING) USING iceberg")
      SparkSQLSubmitter.submitSQLWithSession(
        spark,
        s"INSERT INTO $target VALUES (1, 'Alice'), (2, 'Bob')",
        new util.HashMap[String, String]())

      assert(spark.table(target).count() == 2L)
    } finally dropTable("native_insert")
  }

  test("SparkSQLSubmitter executes native Iceberg REPLACE TABLE AS SELECT") {
    val source = table("native_replace_source")
    val target = table("native_replace_target")
    try {
      spark.sql(s"CREATE TABLE $source (id INT, name STRING) USING iceberg")
      spark.sql(s"INSERT INTO $source VALUES (1, 'new'), (2, 'data')")
      spark.sql(s"CREATE TABLE $target (id INT, name STRING) USING iceberg")
      spark.sql(s"INSERT INTO $target VALUES (0, 'old')")

      SparkSQLSubmitter.submitSQLWithSession(
        spark,
        s"REPLACE TABLE $target USING iceberg AS SELECT * FROM $source",
        new util.HashMap[String, String]())

      assert(spark.table(target).count() == 2L)
      assert(spark.table(target).collect().map(_.getInt(0)).toSet == Set(1, 2))
    } finally {
      dropTable("native_replace_source")
      dropTable("native_replace_target")
    }
  }

  test("native Iceberg table properties are declared in SQL") {
    val target = table("native_table_properties")
    try {
      spark.sql(
        s"""CREATE TABLE $target (id INT, name STRING) USING iceberg
           |TBLPROPERTIES ('write.format.default' = 'parquet')""".stripMargin)

      val properties = spark.sql(s"SHOW TBLPROPERTIES $target").collect()
        .map(row => row.getString(0) -> row.getString(1)).toMap
      assert(properties.get("write.format.default").contains("parquet"))
    } finally dropTable("native_table_properties")
  }

  test("native Iceberg writes create readable snapshots") {
    val target = table("native_snapshots")
    try {
      spark.sql(s"CREATE TABLE $target (id INT) USING iceberg")
      spark.sql(s"INSERT INTO $target VALUES (1)")

      val snapshots = spark.sql(
        s"SELECT snapshot_id FROM $target.snapshots ORDER BY committed_at DESC")
      assert(snapshots.collect().nonEmpty)
    } finally dropTable("native_snapshots")
  }
}
