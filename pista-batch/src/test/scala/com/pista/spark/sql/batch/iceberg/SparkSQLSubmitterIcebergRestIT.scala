package com.pista.spark.sql.batch.iceberg

import com.pista.spark.sql.batch.SparkSQLSubmitter
import com.pista.spark.sql.test.SubmitterIT
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite

import java.util

/**
 * Explicit external validation for the Iceberg REST Catalog deployment shape.
 *
 * Run this suite only with a reachable REST Catalog and configured storage credentials:
 * PISTA_ICEBERG_REST_URI, PISTA_ICEBERG_CATALOG_NAME, and PISTA_ICEBERG_NAMESPACE.
 */
@DoNotDiscover
class SparkSQLSubmitterIcebergRestIT extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private lazy val catalogName = sys.env.getOrElse("PISTA_ICEBERG_CATALOG_NAME", "rest_catalog")
  private lazy val namespace = sys.env.getOrElse("PISTA_ICEBERG_NAMESPACE", "pista_test")
  private lazy val restUri = sys.env.getOrElse(
    "PISTA_ICEBERG_REST_URI",
    throw new IllegalStateException("PISTA_ICEBERG_REST_URI is required for Iceberg REST IT"))

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQLSubmitterIcebergRestIT")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$catalogName.type", "rest")
      .config(s"spark.sql.catalog.$catalogName.uri", restUri)
      .getOrCreate()
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.$namespace")
  }

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  test("SparkSQLSubmitter writes through the configured Iceberg REST Catalog", SubmitterIT) {
    val table = s"$catalogName.$namespace.native_rest_${System.currentTimeMillis()}"
    try {
      SparkSQLSubmitter.submitSQLWithSession(
        spark,
        s"CREATE TABLE $table USING iceberg AS SELECT 1 AS id, 'rest' AS source",
        new util.HashMap[String, String]())

      assert(spark.table(table).count() == 1L)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }
}
