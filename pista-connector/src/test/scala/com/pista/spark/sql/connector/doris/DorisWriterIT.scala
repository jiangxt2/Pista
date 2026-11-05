package com.pista.spark.sql.connector.doris

import com.fasterxml.jackson.databind.ObjectMapper
import com.pista.spark.sql.conf.SubmitterConf
import com.pista.spark.sql.connector.doris.batch.{DorisBatchConfig, DorisTransactionCoordinator}
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import com.pista.spark.sql.test.util.{CloseableGroup, JdbcAssertUtils}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{HttpURLConnection, InetSocketAddress, URL}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Using
import scala.util.control.NonFatal

/** End-to-end public writer coverage, including actual two-phase-commit traffic. */
class DorisWriterIT extends DorisITSupport {
  private val metadataTasks = ListBuffer.empty[String]

  private def target(prefix: String): DorisBatchConfig = {
    val (database, table) = createDorisTable(prefix,
      """(id INT, value VARCHAR(128)) DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ("replication_num" = "2")""".stripMargin)
    batchConfig(database, table, enable2PC = true)
  }

  private def output(config: DorisBatchConfig, forceJdbc: Boolean = false): OutputConfig =
    OutputConfig(Some(s"${config.database}.${config.table}"), "doris", "append", Nil,
      Map("doris.fenodes" -> fenodes, "doris.query.port" -> feQueryPort.toString,
        "doris.user" -> "root", "doris.password" -> "", "force.jdbc" -> forceJdbc.toString))

  private def configured[T](config: DorisBatchConfig, metadata: Boolean = true)(body: => T): T =
    withWriterConfiguration(config) {
      metadataTasks += expectedTaskId(config)
      val key = SubmitterConf.DORIS_CONNECTOR_ENABLE_2PC.key
      val previous = spark.conf.getOption(key)
      if (!metadata) Seq(SubmitterConf.META_HOST, SubmitterConf.META_PORT,
        SubmitterConf.META_DATABASE, SubmitterConf.META_USERNAME, SubmitterConf.META_PASSWORD)
        .foreach(entry => spark.conf.unset(entry.key))
      spark.conf.set(key, "true")
      try body
      finally previous match {
        case Some(value) => spark.conf.set(key, value)
        case None => spark.conf.unset(key)
      }
    }

  test("Doris writer writes and can be queried via FE JDBC") {
    val config = target("it_doris_writer")
    WriterRegistry.write(twoColumnSource(1 to 20), output(config, forceJdbc = true))
    assert(queryLong(s"SELECT COUNT(*) FROM `${config.database}`.`${config.table}`") == 20L)
  }

  test("public one-row 2PC append precommits and commits the same transaction without metadata") {
    val config = target("it_doris_public_2pc")
    withProbes(config) { (routed, probes) =>
      configured(routed, metadata = false) {
        WriterRegistry.write(twoColumnSource(Seq(1)), output(config))
      }
      assert(queryLong(s"SELECT COUNT(*) FROM `${config.database}`.`${config.table}`") == 1L)
      assertProtocol(config, probes)
    }
  }

  test("public 2PC configuration conflicts leave target, transactions, and metadata untouched") {
    val config = target("it_doris_2pc_conflict")
    withProbes(config) { (routed, probes) =>
      Seq(
        output(config, forceJdbc = true),
        output(config).copy(path = Some("other.target")),
        output(config).copy(path = Some(config.table)))
        .foreach { request =>
          configured(routed) {
            intercept[RuntimeException](WriterRegistry.write(twoColumnSource(Seq(1)), request))
          }
        }
      configured(routed) {
        spark.conf.unset(SubmitterConf.DORIS_DATABASE.key)
        intercept[RuntimeException](WriterRegistry.write(twoColumnSource(Seq(1)), output(config)))
      }
      assert(probes.forall(_.requests.get() == 0))
      assert(queryLong(s"SELECT COUNT(*) FROM `${config.database}`.`${config.table}`") == 0L)
      JdbcAssertUtils.withConnection(metaJdbcUrl, metaUser, metaPassword) { connection =>
        val statement = connection.prepareStatement("SELECT COUNT(*) FROM doris_task_info WHERE task_id = ?")
        try {
          statement.setString(1, expectedTaskId(config))
          val result = statement.executeQuery()
          try { assert(result.next()); assert(result.getLong(1) == 0L) }
          finally result.close()
        } finally statement.close()
      }
    }
  }

  test("public 2PC metadata skips the same task retry and appends a distinct business batch") {
    val config = target("it_doris_2pc_batches")
    configured(config) {
      WriterRegistry.write(twoColumnSource(Seq(11)), output(config))
      assertTaskState(config, status = 1, writtenRows = 1L)
      WriterRegistry.write(twoColumnSource(Seq(11)), output(config))
    }
    val nextBatch = config.copy(labelPrefix = config.labelPrefix + "_next")
    configured(nextBatch) {
      WriterRegistry.write(twoColumnSource(Seq(21)), output(config))
      assertTaskState(nextBatch, status = 1, writtenRows = 2L)
    }
    assert(queryColumn(s"SELECT id FROM `${config.database}`.`${config.table}` ORDER BY id")(
      _.getInt(1)) == Seq(11, 21))
  }

  private def transactionState(database: String, id: Long): String =
    queryColumn(s"SHOW TRANSACTION FROM `$database` WHERE id = $id")(
      _.getString("TransactionStatus")).headOption.getOrElse(fail(s"Missing transaction $id"))

  private case class ProtocolEvent(id: Long, operation: String, twoPhase: Boolean, state: String)

  private def assertProtocol(config: DorisBatchConfig, probes: Seq[RecordingProxy]): Unit = {
    probes.foreach(probe => assert(probe.failures.isEmpty, probe.failures.asScala.map(_.toString).mkString(",")))
    val events = probes.flatMap(_.events.asScala)
    val loads = events.filter(_.operation == "load")
    assert(loads.nonEmpty)
    assert(loads.forall(event => event.twoPhase && event.state == "PRECOMMITTED"))
    assert(loads.map(_.id).toSet == events.filter(_.operation == "commit").map(_.id).toSet)
    loads.foreach { event =>
      assert(transactionState(config.database, event.id) == "VISIBLE")
      val commit = events.find(other => other.id == event.id && other.operation == "commit").get
      info(s"txn=${event.id} two_phase_commit=${event.twoPhase} loadState=${event.state} " +
        s"commitState=${commit.state} finalState=VISIBLE scalaRuntime=${scala.util.Properties.versionNumberString}")
    }
  }

  private def withProbes[T](config: DorisBatchConfig)(body: (DorisBatchConfig, Seq[RecordingProxy]) => T): T =
    Using.resource(CloseableGroup.create()) { resources =>
      val probes = (fenodes +: benodes).map(endpoint => resources.register(new RecordingProxy(endpoint, config)))
      body(config.copy(fenodes = probes.head.endpoint, benodes = probes.tail.map(_.endpoint).mkString(",")), probes)
    }

  /** Forwards unchanged entities; records only transaction IDs, phase headers, and independent SQL states. */
  private class RecordingProxy(upstream: String, config: DorisBatchConfig) extends AutoCloseable {
    val requests = new AtomicInteger()
    val events = new ConcurrentLinkedQueue[ProtocolEvent]()
    val failures = new ConcurrentLinkedQueue[Throwable]()
    private val transactionIds = new ConcurrentLinkedQueue[Long]()
    private val mapper = new ObjectMapper()
    private val executor = Executors.newCachedThreadPool()
    private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.setExecutor(executor)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = forward(exchange)
    })
    server.start()
    def endpoint: String = s"127.0.0.1:${server.getAddress.getPort}"

    private def forward(exchange: HttpExchange): Unit = {
      var connection: HttpURLConnection = null
      try {
        requests.incrementAndGet()
        val path = exchange.getRequestURI.toString
        connection = new URL(s"http://$upstream$path").openConnection().asInstanceOf[HttpURLConnection]
        connection.setInstanceFollowRedirects(false)
        connection.setConnectTimeout(10000)
        connection.setReadTimeout(30000)
        connection.setRequestMethod(exchange.getRequestMethod)
        val hopHeaders = Set("host", "connection", "content-length", "transfer-encoding", "expect")
        exchange.getRequestHeaders.asScala.foreach { case (key, values) =>
          if (!hopHeaders(key.toLowerCase(java.util.Locale.ROOT)))
            values.asScala.foreach(connection.addRequestProperty(key, _))
        }
        if (Set("PUT", "POST")(exchange.getRequestMethod)) {
          val bytes = exchange.getRequestBody.readAllBytes()
          connection.setDoOutput(true)
          connection.setFixedLengthStreamingMode(bytes.length)
          val output = connection.getOutputStream
          try output.write(bytes) finally output.close()
        }
        val status = connection.getResponseCode
        val input = if (status >= 400) connection.getErrorStream else connection.getInputStream
        val response = if (input == null) Array.empty[Byte] else try input.readAllBytes() finally input.close()
        val encoding = Option(connection.getHeaderField("Content-Encoding"))
        if (status == 200 && (path.endsWith("/_stream_load") || path.endsWith("/_stream_load_2pc"))) {
          val decoded = if (encoding.exists(_.equalsIgnoreCase("gzip"))) {
            val gzip = new GZIPInputStream(new java.io.ByteArrayInputStream(response))
            try gzip.readAllBytes() finally gzip.close()
          } else response
          val json = mapper.readTree(new String(decoded, UTF_8))
          val headers = exchange.getRequestHeaders
          val id = Option(headers.getFirst("txn_id")).orElse(Option(json.get("TxnId")).map(_.asText()))
          id.foreach { value =>
            val txn = value.toLong
            transactionIds.add(txn)
            val operation = Option(headers.getFirst("txn_operation")).getOrElse("load")
            val state = transactionState(config.database, txn)
            val twoPhase = Option(headers.getFirst("two_phase_commit")).contains("true")
            events.add(ProtocolEvent(txn, operation, twoPhase, state))
          }
        }
        Seq("Content-Type", "Content-Encoding", "Location").foreach { key =>
          Option(connection.getHeaderField(key)).foreach(exchange.getResponseHeaders.set(key, _))
        }
        exchange.sendResponseHeaders(status, response.length)
        exchange.getResponseBody.write(response)
      } catch {
        case NonFatal(error) =>
          failures.add(error)
          exchange.sendResponseHeaders(502, -1)
      } finally {
        if (connection != null) connection.disconnect()
        exchange.close()
      }
    }

    override def close(): Unit = {
      try {
        class TransactionCleanup extends DorisTransactionCoordinator {
          def abort(id: Long): Boolean = send2pcAbort(id.toString, config)
        }
        val cleanup = new TransactionCleanup
        transactionIds.asScala.toSet.foreach { id: Long =>
          if (transactionState(config.database, id) == "PRECOMMITTED") {
            assert(cleanup.abort(id))
            assert(transactionState(config.database, id) == "ABORTED")
          }
        }
      } finally {
        server.stop(0)
        executor.shutdownNow()
      }
    }
  }

  override def afterAll(): Unit = {
    try JdbcAssertUtils.withConnection(metaJdbcUrl, metaUser, metaPassword) { connection =>
      val statement = connection.prepareStatement("DELETE FROM doris_task_info WHERE task_id = ?")
      try metadataTasks.distinct.foreach { task =>
        statement.setString(1, task)
        statement.executeUpdate()
      } finally statement.close()
    } finally super.afterAll()
  }
}
