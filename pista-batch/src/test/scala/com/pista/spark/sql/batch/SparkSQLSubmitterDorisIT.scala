package com.pista.spark.sql.batch

import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.TestDataGenerator

class SparkSQLSubmitterDorisIT extends BatchSubmitterIT {
  test("SparkSQLSubmitter within Spark 3.5.8 container Spark 3.5.8 output to container Doris") {
    val doris = ContainerSuite.doris
    val table = TestDataGenerator.uniqueName("it_submit_doris").toLowerCase
    val connection = java.sql.DriverManager.getConnection(doris.mysqlJdbcUrl, "root", "")
    try {
      val statement = connection.createStatement()
      try {
        statement.execute("CREATE DATABASE IF NOT EXISTS pista_test")
        statement.execute(
          s"""CREATE TABLE pista_test.$table
             |(id INT, value VARCHAR(128))
             |DUPLICATE KEY(id)
             |DISTRIBUTED BY HASH(id) BUCKETS 2
             |PROPERTIES ("replication_num" = "2")""".stripMargin)
      } finally statement.close()
    } finally connection.close()

    submit(
      "SELECT id, value FROM VALUES (1, 'one'), (2, 'two') AS input(id, value)",
      Map(
        "spark.pista.output.format" -> "doris",
        "spark.pista.output.path" -> s"pista_test.$table",
        "spark.pista.output.mode" -> "append",
        "spark.pista.doris.fenodes" -> doris.internalFeEndpoint,
        "spark.pista.doris.database" -> "pista_test",
        "spark.pista.doris.table" -> table,
        "spark.pista.doris.feQueryPort" -> "9030",
        "spark.pista.doris.user" -> "root",
        "spark.pista.doris.password" -> "",
        // DorisWriter's small-data path uses OutputConfig.options for JDBC.
        "spark.pista.output.options.doris.fenodes" -> doris.internalFeEndpoint,
        "spark.pista.output.options.doris.query.port" -> "9030",
        "spark.pista.output.options.doris.user" -> "root",
        "spark.pista.output.options.doris.password" -> ""))
    val result = connectionForCount(doris.mysqlJdbcUrl, s"SELECT count() FROM pista_test.$table")
    assert(result == 2L)
  }

  private def connectionForCount(url: String, sql: String): Long = {
    val connection = java.sql.DriverManager.getConnection(url, "root", "")
    try {
      val result = connection.createStatement().executeQuery(sql)
      try { assert(result.next()); result.getLong(1) }
      finally result.close()
    } finally connection.close()
  }
}
