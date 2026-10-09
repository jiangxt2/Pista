package com.pista.spark.sql.connector.doris

import com.pista.spark.errors.{PistaConfigException, PistaDorisException}
import com.pista.spark.sql.conf.SubmitterConf
import com.pista.spark.sql.doris.meta.{DorisFENodeResolver, DorisMetaConf, DorisMetaConnection, DorisMetaManager}
import com.pista.spark.sql.execution.datasources.writer.{DataStats, OutputConfig, WriterRegistry}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DateType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.{PrintWriter, StringWriter}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.SQLException
import org.slf4j.Logger
import java.util.Properties
import scala.collection.mutable.ListBuffer

class DorisWriterSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("DorisWriterSuite")
      .config("spark.ui.enabled", "false").getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def output(mode: String, forceJdbc: Boolean = false): OutputConfig =
    OutputConfig(Some("pista_test.target"), "doris", mode, Nil,
      Map("force.jdbc" -> forceJdbc.toString, "doris.fenodes" -> "127.0.0.1:8030"))

  private class RoutingWriter extends DorisWriter {
    var connectorCalled = false
    var jdbcCalled = false
    var connectorDf: DataFrame = _
    def route(df: DataFrame, config: OutputConfig): Unit =
      writeInternal(df, DataStats.empty.copy(rowCount = Some(1L)), config)
    def connector(df: DataFrame, config: OutputConfig): Unit = super.writeViaConnector(df, config)
    override protected def writeViaConnector(df: DataFrame, config: OutputConfig): Unit = {
      connectorCalled = true
      connectorDf = df
    }
    override protected def writeViaJdbc(df: DataFrame, url: String, table: String,
      batchSize: Int, properties: Properties, mode: String): Unit = jdbcCalled = true
  }

  test("generic overwrite selects atomic connector routing even for small forced-JDBC output") {
    val session = spark.newSession()
    val writer = new RoutingWriter
    writer.route(session.range(1).toDF(), output("OvErWrItE", forceJdbc = true))
    assert(writer.connectorCalled)
    assert(!writer.jdbcCalled)
  }

  test("Doris overwrite and partition conversion read the active session configuration") {
    val session = spark.newSession()
    session.conf.set(SubmitterConf.DORIS_OVERWRITE.key, "true")
    session.conf.set(SubmitterConf.DORIS_PARTITION_COLUMN.key, "biz_date")
    val writer = new RoutingWriter
    writer.route(session.sql("SELECT '20261001' AS biz_date"), output("append"))
    assert(writer.connectorCalled)
    assert(writer.connectorDf.schema("biz_date").dataType == DateType)
  }

  test("append preserves the small-data JDBC route when no overwrite is requested") {
    val writer = new RoutingWriter
    writer.route(spark.newSession().range(1).toDF(), output("append"))
    assert(writer.jdbcCalled)
    assert(!writer.connectorCalled)
  }

  test("generic overwrite cannot hide an invalid Doris overwrite setting") {
    val session = spark.newSession()
    session.conf.set(SubmitterConf.DORIS_OVERWRITE.key, "invalid")
    val writer = new RoutingWriter
    intercept[IllegalArgumentException](writer.route(session.range(1).toDF(), output("overwrite")))
    assert(!writer.connectorCalled && !writer.jdbcCalled)
  }

  test("generic and Doris overwrite reject conflicting target settings") {
    Seq(("overwrite", false), ("append", true)).foreach { case (mode, dorisOverwrite) =>
      val session = spark.newSession()
      session.conf.set(SubmitterConf.DORIS_FENODES.key, "127.0.0.1:8030")
      session.conf.set(SubmitterConf.DORIS_DATABASE.key, "pista_test")
      session.conf.set(SubmitterConf.DORIS_TABLE.key, "other")
      session.conf.set(SubmitterConf.DORIS_OVERWRITE.key, dorisOverwrite.toString)
      val (failure, messages) = DorisLogCapture { sink =>
        val writer = new RoutingWriter {
          override protected def log: Logger = sink
        }
        intercept[PistaDorisException](writer.connector(session.range(1).toDF(), output(mode)))
      }
      assert(failure.getMessage.contains("Conflicting Doris overwrite target"))
      assert(messages.count(_.contains("stage=configuration.target outcome=failed")) == 1)
      assert(!messages.mkString.contains("pista_test.other"))
      assert(!messages.mkString.contains("pista_test.target"))
    }
  }

  test("partial metadata configuration reports every missing key without attempting a target write") {
    val session = spark.newSession()
    session.conf.set(SubmitterConf.META_HOST.key, "127.0.0.1")
    val (failure, messages) = DorisLogCapture { sink =>
      val writer = new DorisWriter {
        override protected def log: Logger = sink
      }
      intercept[PistaDorisException](writer.write(session.range(1).toDF(), output("overwrite")))
    }
    Seq(SubmitterConf.META_DATABASE.key, SubmitterConf.META_USERNAME.key,
      SubmitterConf.META_PASSWORD.key, SubmitterConf.DORIS_CLUSTER_NAME.key).foreach { key =>
      assert(failure.getMessage.contains(key))
      assert(messages.exists(_.contains(key)))
    }
    assert(!failure.getMessage.contains("127.0.0.1"))
    assert(messages.count(_.contains("stage=metadata.configure outcome=failed")) == 1)
    assert(!messages.mkString.contains("127.0.0.1"))
  }

  test("public registry rejects partial metadata configuration while keeping values redacted") {
    val session = spark.newSession()
    session.conf.set(SubmitterConf.META_HOST.key, "127.0.0.1")
    session.conf.set(SubmitterConf.META_PASSWORD.key, "test-password")
    val failure = intercept[RuntimeException] {
      WriterRegistry.write(session.range(1).toDF(), output("overwrite"))
    }
    assert(failure.getMessage.contains("PistaDorisException"))
    assert(!failure.getMessage.contains("127.0.0.1"))
    assert(!failure.getMessage.contains("test-password"))
  }

  test("completely absent metadata remains optional and proceeds to target configuration validation") {
    val session = spark.newSession()
    session.conf.set(SubmitterConf.DORIS_FENODES.key, "127.0.0.1:8030")
    val writer = new RoutingWriter
    val failure = intercept[PistaConfigException] {
      writer.connector(session.range(1).toDF(), output("overwrite"))
    }
    assert(failure.getMessage.contains(SubmitterConf.DORIS_DATABASE.key))
  }

  test("metadata errors preserve SQLSTATE and cause while redacting values from logs") {
    val conf = DorisMetaConf("127.0.0.1", 5432, "pista_meta", "pista", "test-password", "pista_it")
    val original = new SQLException("private-value", "08006")
    val connection = new DorisMetaConnection(conf) {
      override def getConnection: java.sql.Connection = throw original
    }
    val (failure, messages) = DorisLogCapture { sink =>
      val manager = new DorisMetaManager(connection, conf) {
        override protected def log: Logger = sink
      }
      intercept[PistaDorisException] {
        manager.upsertRunning("private-task", "pista_test", "target", "20261001",
          "spark_connector", None, Some("biz_date = '20261001'"))
      }
    }
    assert(failure.getCause eq original)
    assert(failure.getMessage.contains("sqlState=08006"))
    assert(messages.count(_.contains("stage=metadata.running")) == 1)
    assert(messages.exists(_.contains("rdateChars=8")))
    val text = messages.mkString
    Seq("private-value", "private-task", "20261001", "test-password", "127.0.0.1").foreach { value =>
      assert(!text.contains(value))
    }
  }

  test("FE metadata query errors remain failures and retain their root cause") {
    val conf = DorisMetaConf("127.0.0.1", 5432, "pista_meta", "pista", "", "pista_it")
    val original = new SQLException("private-value", "08006")
    val connection = new DorisMetaConnection(conf) {
      override def getConnection: java.sql.Connection = throw original
    }
    val (failure, messages) = DorisLogCapture { sink =>
      val resolver = new DorisFENodeResolver(Some(connection)) {
        override protected def log: Logger = sink
      }
      intercept[PistaDorisException](resolver.resolve(None, "pista_it"))
    }
    assert(failure.getCause eq original)
    assert(failure.getMessage.contains("sqlState=08006"))
    assert(!messages.mkString.contains("private-value"))
  }
}

/** Capture the actual SLF4J calls and complete throwables independently of a test JVM's backend. */
object DorisLogCapture {
  def apply[T](body: Logger => T): (T, Seq[String]) = {
    val events = ListBuffer.empty[String]
    val handler = new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, arguments: Array[AnyRef]): AnyRef =
        method.getName match {
          case name if name.startsWith("is") && name.endsWith("Enabled") => java.lang.Boolean.TRUE
          case "getName" => "doris-capture"
          case "toString" => "doris-capture"
          case "hashCode" => Int.box(System.identityHashCode(proxy))
          case "equals" => Boolean.box(proxy.asInstanceOf[AnyRef] eq arguments(0))
          case name =>
            val args = Option(arguments).map(_.toSeq).getOrElse(Seq.empty)
            val message = args.collectFirst { case text: String => text }.getOrElse("")
            val output = new StringWriter()
            args.collect { case error: Throwable => error }.foreach(_.printStackTrace(new PrintWriter(output)))
            events += s"$name $message\n$output"
            null
        }
    }
    val sink = Proxy.newProxyInstance(classOf[Logger].getClassLoader, Array(classOf[Logger]), handler)
      .asInstanceOf[Logger]
    val result = body(sink)
    (result, events.toList)
  }
}
