package com.pista.spark.sql.test.base

import com.pista.spark.sql.functions.PistaFunctionInstaller
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

/** SparkSession base for connector-level ITs. */
trait SparkBaseIT extends BaseIT {
  protected def useBaseSparkSession: Boolean = true
  protected def sparkSessionConf: Map[String, String] = Map.empty

  private val baseExpectedConfigs = Map(
    "spark.ui.enabled" -> "false",
    "spark.sql.shuffle.partitions" -> "4")

  protected lazy val spark: SparkSession = {
    val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    val session = existing match {
      case Some(value) if sessionMatches(value) => value
      case Some(value) =>
        value.stop()
        createSparkSession()
      case None => createSparkSession()
    }
    PistaFunctionInstaller.install(session)
    session
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (useBaseSparkSession) spark
  }

  override def afterAll(): Unit = {
    try {
      if (useBaseSparkSession) {
        SparkSession.getActiveSession.foreach(_.stop())
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    } finally super.afterAll()
  }

  private def sessionMatches(session: SparkSession): Boolean =
    (baseExpectedConfigs ++ sparkSessionConf).forall { case (key, expected) =>
      session.conf.getOption(key).contains(expected)
    }

  private def createSparkSession(): SparkSession = {
    val warehouse = Files.createTempDirectory("pista-it-warehouse-")
    com.pista.spark.sql.test.container.ContainerSuite.registerPath(warehouse)
    val builder = SparkSession.builder()
      .appName("pista-it")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", warehouse.toString)
    sparkSessionConf.foreach { case (key, value) => builder.config(key, value) }
    builder.getOrCreate()
  }
}
