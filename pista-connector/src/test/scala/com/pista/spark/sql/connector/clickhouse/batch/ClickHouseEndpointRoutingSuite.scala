package com.pista.spark.sql.connector.clickhouse.batch

import org.scalatest.funsuite.AnyFunSuite

class ClickHouseEndpointRoutingSuite extends AnyFunSuite {
  test("column contains port number when both host and port are replaced") {
    val result = ClickHouseConcurrentWriter.replaceHost(
      "jdbc:clickhouse://placeholder:8123/pista_test?x=1", "127.0.0.1:49123")
    assert(result == "jdbc:clickhouse://127.0.0.1:49123/pista_test?x=1")
  }

  test("Pure host input maintains the original port of the JDBC URL") {
    val result = ClickHouseConcurrentWriter.replaceHost(
      "jdbc:clickhouse://placeholder:8123/pista_test", "clickhouse-node")
    assert(result == "jdbc:clickhouse://clickhouse-node:8123/pista_test")
  }

  test("IPv6 host:port in JDBC bracket format") {
    val result = ClickHouseConcurrentWriter.replaceHost(
      "jdbc:clickhouse://placeholder:8123/pista_test", "[::1]:8123")
    assert(result == "jdbc:clickhouse://[::1]:8123/pista_test")
  }
}
