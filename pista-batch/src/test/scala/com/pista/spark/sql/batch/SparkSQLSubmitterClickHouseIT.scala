package com.pista.spark.sql.batch

import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.TestDataGenerator

class SparkSQLSubmitterClickHouseIT extends BatchSubmitterIT {
  test("SparkSQLSubmitter outputs ClickHouse in a Spark 3.5.8 container") {
    val clickhouse = ContainerSuite.clickhouse
    val table = TestDataGenerator.uniqueName("it_submit_ck").toLowerCase
    clickhouse.executeOnEntry(
      s"""CREATE TABLE pista_test.$table ON CLUSTER ck_cluster
         |(id Int32, value String)
         |ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$table', '{replica}')
         |ORDER BY id""".stripMargin)
    submit(
      "SELECT id, value FROM VALUES (1, 'one'), (2, 'two') AS input(id, value)",
      Map(
        "spark.pista.output.format" -> "clickhouse",
        "spark.pista.output.path" -> s"pista_test.$table",
        "spark.pista.output.mode" -> "append",
        "spark.pista.clickhouse.machine.count" -> "1",
        "spark.pista.clickhouse.host" -> clickhouse.internalNodeHost(0),
        "spark.pista.clickhouse.port" -> "8123",
        "spark.pista.output.options.clickhouse.username" -> clickhouse.username,
        "spark.pista.output.options.clickhouse.password" -> clickhouse.userPassword))
    assert(clickhouse.queryLong(s"SELECT count() FROM pista_test.$table") == 2L)
  }
}
