package com.pista.spark.sql.connector.clickhouse

import org.scalatest.funsuite.AnyFunSuite

import java.util.{Properties, UUID}
import scala.collection.JavaConverters._

class ClickHouseReaderPropertiesSuite extends AnyFunSuite {
  private class TestableClickHouseReader extends ClickHouseReader {
    def properties(options: Map[String, String]): Properties = buildJdbcProperties(options)
  }
  private val reader = new TestableClickHouseReader

  test("JDBC reader leaves provider, timeout, and absent credentials to the driver") {
    assert(reader.properties(Map.empty).isEmpty)
    val properties = reader.properties(Map(
      "http_connection_provider" -> "HTTP_URL_CONNECTION", "socket_timeout" -> "123"))
    assert(properties.isEmpty)
  }

  test("JDBC reader forwards only explicitly configured authentication with existing precedence") {
    val password = UUID.randomUUID().toString
    val otherPassword = UUID.randomUUID().toString
    val properties = reader.properties(Map(
      "clickhouse.username" -> "preferred", "clickhouse.user" -> "second", "user" -> "third",
      "clickhouse.password" -> password, "password" -> otherPassword,
      "http_connection_provider" -> "HTTP_URL_CONNECTION", "socket_timeout" -> "123"))
    assert(properties.stringPropertyNames().asScala.toSet == Set("user", "password"))
    assert(properties.getProperty("user") == "preferred")
    assert(properties.getProperty("password") == password)

    val secondary = reader.properties(Map("clickhouse.user" -> "second", "user" -> "third"))
    assert(secondary.getProperty("user") == "second")
    assert(secondary.getProperty("password") == "")
    val unprefixed = reader.properties(Map("user" -> "third", "password" -> password))
    assert(unprefixed.getProperty("user") == "third")
    assert(unprefixed.getProperty("password") == password)
    val passwordOnly = reader.properties(Map("clickhouse.password" -> password))
    assert(passwordOnly.getProperty("user") == "default")
    assert(passwordOnly.getProperty("password") == password)
  }
}
