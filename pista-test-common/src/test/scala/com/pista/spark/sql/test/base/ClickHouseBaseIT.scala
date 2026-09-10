package com.pista.spark.sql.test.base

import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.{JdbcAssertUtils, MetaSchemaInitializer, TestDataGenerator}

import java.sql.Connection
import scala.collection.mutable.ListBuffer

trait ClickHouseBaseIT extends SparkBaseIT {
  protected lazy val ckContainer = ContainerSuite.clickhouse
  protected lazy val pgContainer = ContainerSuite.postgres
  private val tables = ListBuffer.empty[(String, String)]

  override def beforeAll(): Unit = {
    super.beforeAll()
    ckContainer
    pgContainer
    MetaSchemaInitializer.initializeClickHouse(
      pgContainer.metaJdbcUrl, pgContainer.metaUser, pgContainer.metaPassword, ckContainer)
  }

  override def afterAll(): Unit = {
    try tables.reverse.foreach { case (database, table) =>
      scala.util.Try(ckContainer.executeOnEntry(
        s"DROP TABLE IF EXISTS $database.$table ON CLUSTER ck_cluster SYNC"))
    }
    finally super.afterAll()
  }

  protected def registerClickHouseTable(database: String, table: String): Unit =
    tables += ((database, table))

  protected def uniqueClickHouseTable(prefix: String = "it_tbl"): (String, String) = {
    val database = "pista_test"
    val table = TestDataGenerator.uniqueName(prefix).toLowerCase
    registerClickHouseTable(database, table)
    (database, table)
  }

  protected def entryJdbcUrl: String = ckContainer.entryJdbcUrl
  protected def clickHouseNodes: Seq[String] = ckContainer.nodeAddresses
  protected def machineRecords = ckContainer.machineRecords
  protected def metaJdbcUrl: String = pgContainer.metaJdbcUrl
  protected def metaUser: String = pgContainer.metaUser
  protected def metaPassword: String = pgContainer.metaPassword

  protected def withClickHouseConnection[T](f: Connection => T): T =
    JdbcAssertUtils.withConnection(entryJdbcUrl, ckContainer.username, ckContainer.userPassword)(f)

  protected def assertClickHouseCount(database: String, table: String, expected: Long): Unit =
    withClickHouseConnection { connection =>
      JdbcAssertUtils.assertCount(connection, s"SELECT count() FROM $database.$table", expected)
    }

  override protected def sparkSessionConf: Map[String, String] = {
    val meta = pgContainer
    Map(
      "spark.pista.meta.host" -> meta.getHost,
      "spark.pista.meta.port" -> meta.getMappedPort(5432).toString,
      "spark.pista.meta.database" -> "pista_meta",
      "spark.pista.meta.username" -> metaUser,
      "spark.pista.meta.password" -> metaPassword)
  }
}
