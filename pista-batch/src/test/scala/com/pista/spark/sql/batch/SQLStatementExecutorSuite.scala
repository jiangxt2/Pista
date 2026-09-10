package com.pista.spark.sql.batch

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * SQLStatementExecutor Test
 *
 * Overwrite: selected as a multi-classification of SELECT/INSERT/DDL.isSelectQuery classification overwriting into three categories: SELECT, INSERT, and DDL.SELECT / INSERT / DDL)
 */
class SQLStatementExecutorSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder.master("local[2]").enableHiveSupport.getOrCreate
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    // SparkSession current fork JVM manage uniformly, cannot be shared across sessions during suite stop sharing SparkContext.
  }

  // ==================== isSelectQuery Multiclass ====================

  test("isSelectQuery returns true for SELECT") {
    val df = spark.sql("SELECT 1 AS id, 'hello' AS name")
    assert(SQLStatementExecutor.isSelectQuery(df) === true)
  }

  test("isSelectQuery: DDL statement returns false") {
    val df = spark.sql("SHOW DATABASES")
    // SHOW DATABASES is Commandis not a statement SELECT
    assert(SQLStatementExecutor.isSelectQuery(df) === false)
  }
}
