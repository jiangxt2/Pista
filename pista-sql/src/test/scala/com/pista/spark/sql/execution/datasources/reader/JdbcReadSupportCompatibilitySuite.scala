package com.pista.spark.sql.execution.datasources.reader

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class JdbcReadSupportCompatibilitySuite extends AnyFunSuite {
  test("JDBC read helper retains its original JVM signature") {
    val method = classOf[JdbcReadSupport].getDeclaredMethod("readViaJdbc",
      classOf[SparkSession], classOf[String], classOf[String], classOf[java.util.Properties],
      classOf[Option[_]], classOf[Seq[_]], java.lang.Integer.TYPE,
      classOf[Option[_]], classOf[Option[_]], classOf[Option[_]])
    assert(method.getReturnType == classOf[org.apache.spark.sql.Dataset[_]])
  }
}
