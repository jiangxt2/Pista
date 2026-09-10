package com.pista.spark.sql.test.base

import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.{JdbcAssertUtils, MetaSchemaInitializer, TestDataGenerator}

import java.sql.Connection
import scala.collection.mutable.ListBuffer

trait DorisBaseIT extends SparkBaseIT {
  protected lazy val dorisContainer = ContainerSuite.doris
  protected lazy val pgContainer = ContainerSuite.postgres
  private val tables = ListBuffer.empty[(String, String)]

  override def beforeAll(): Unit = {
    super.beforeAll()
    dorisContainer
    pgContainer
    MetaSchemaInitializer.initializeDoris(
      pgContainer.metaJdbcUrl, pgContainer.metaUser, pgContainer.metaPassword, dorisContainer)
  }

  override def afterAll(): Unit = {
    try tables.reverse.foreach { case (database, table) =>
      scala.util.Try(withDorisConnection { connection =>
        JdbcAssertUtils.execute(connection, s"DROP TABLE IF EXISTS $database.$table")
      })
    }
    finally super.afterAll()
  }

  protected def uniqueDorisTable(prefix: String = "it_tbl"): (String, String) = {
    val database = "pista_test"
    val table = TestDataGenerator.uniqueName(prefix).toLowerCase
    tables += ((database, table))
    (database, table)
  }

  protected def fenodes: String = dorisContainer.feHttpEndpoint
  protected def feQueryPort: Int = dorisContainer.feQueryPort
  protected def benodes: Seq[String] = dorisContainer.beHttpEndpoints
  protected def mysqlJdbcUrl: String = dorisContainer.mysqlJdbcUrl
  protected def metaJdbcUrl: String = pgContainer.metaJdbcUrl
  protected def metaUser: String = pgContainer.metaUser
  protected def metaPassword: String = pgContainer.metaPassword
  protected def autoRedirect: String = "true"

  protected def withDorisConnection[T](f: Connection => T): T =
    JdbcAssertUtils.withConnection(mysqlJdbcUrl, "root", "")(f)
}
