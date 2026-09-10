package com.pista.spark.sql.test.util

import java.sql.{Connection, DriverManager, ResultSet}

object JdbcAssertUtils {
  def withConnection[T](url: String, user: String, password: String)(f: Connection => T): T = {
    val connection = DriverManager.getConnection(url, user, password)
    try f(connection)
    finally connection.close()
  }

  def queryLong(connection: Connection, sql: String): Long = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(sql)
      try {
        require(result.next(), s"Query returned no rows: $sql")
        result.getLong(1)
      } finally result.close()
    } finally statement.close()
  }

  def assertCount(connection: Connection, sql: String, expected: Long): Unit = {
    val actual = queryLong(connection, sql)
    assert(actual == expected, s"Expected count $expected but got $actual for: $sql")
  }

  def execute(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try statement.execute(sql)
    finally statement.close()
  }

  def firstRow(connection: Connection, sql: String)(f: ResultSet => Unit): Unit = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(sql)
      try {
        require(result.next(), s"Query returned no rows: $sql")
        f(result)
      } finally result.close()
    } finally statement.close()
  }
}
