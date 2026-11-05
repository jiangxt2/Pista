package com.pista.spark.sql.connector.doris

import com.fasterxml.jackson.databind.ObjectMapper
import com.pista.spark.sql.connector.doris.batch._
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.test.util.TestDataGenerator

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, SQLException}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.mutable.ListBuffer
import java.util.Base64

class DorisTransactionIT extends DorisITSupport {

  override protected def sparkSessionConf: Map[String, String] = Map("spark.master" -> "local[2,2]")

  test("Doris Connector 2PC transaction visible after commit VISIBLE data visible") {
    val (database, table) = createTransactionTable("it_doris_2pc_commit")
    val config = batchConfig(
      database,
      table,
      labelPrefix = TestDataGenerator.uniqueName("pista_it_2pc_commit"),
      enable2PC = true)

    var runtimePrefix = ""
    val result = trackedWriter(config, twoColumnSource(1 to 20)) { writeConfig =>
      runtimePrefix = writeConfig.labelPrefix
    }.write()

    assert(result.success)
    assert(result.rowCount == 20L)
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 20L)

    val statuses = queryColumn(
      s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '${runtimePrefix}-%'")(_.getString("TransactionStatus"))
    assert(statuses.nonEmpty)
    assert(statuses.forall(_ == "VISIBLE"), s"Transaction status should all be VISIBLE, actual is $statuses")
  }

  test("Doris PRECOMMITTED transactions can be aborted by label without data leakage label abort data leakage is prevented") {
    val (database, table) = createTransactionTable("it_doris_2pc_abort")
    val config = batchConfig(
      database,
      table,
      labelPrefix = TestDataGenerator.uniqueName("pista_it_2pc_abort"),
      enable2PC = true)

    val scope = DorisTransactionScope.create(config.labelPrefix)
    val txnId = createPreCommittedTransaction(config.copy(labelPrefix = scope.labelPrefix + "-pending"), "901,precommitted\n")
    val coordinator = new DorisTransactionCoordinator()
    assert(coordinator.queryTransactionStatuses(config, scope).contains("PRECOMMITTED"))
    assert(coordinator.abortByLabel(config, scope) == 1)
    assert(coordinator.queryTransactionStatuses(config, scope).contains("ABORTED"),
      s"transaction $txnId has not converged ABORTED")
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
  }

  test("rejected transaction query cannot validate an invisible PRECOMMITTED transaction") {
    val (database, table) = createTransactionTable("it_doris_query_failure")
    val config = batchConfig(database, table, enable2PC = true)
    val coordinator = new DorisTransactionCoordinator()
    val scope = DorisTransactionScope.create(config.labelPrefix)
    val txnId = createPreCommittedTransaction(config.copy(labelPrefix = scope.labelPrefix + "-pending"), "901,pending\n")
    try {
      assert(transactionState(config, txnId) == "PRECOMMITTED")
      val failure = intercept[SQLException] {
        coordinator.validateAllVisible(config.copy(password = "test-password"), scope, maxWaitMs = 1000)
      }
      assert(failure.getErrorCode == 1045 && failure.getSQLState == "28000")
      assert(transactionState(config, txnId) == "PRECOMMITTED")
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
    } finally {
      abortExact(config, txnId)
    }
  }

  test("transaction verification authentication failure records FAILURE after publication") {
    val (database, table) = createTransactionTable("it_doris_verify_failure")
    val config = batchConfig(database, table, enable2PC = true)
    withMetadataContext() { context =>
      val writer = new DorisConcurrentWriter(config, context, twoColumnSource(1 to 2), System.currentTimeMillis()) {
        override protected def createTransactionCoordinator(): DorisTransactionCoordinator =
          new DorisTransactionCoordinator {
            override protected def buildDorisJdbcConnection(
              config: DorisBatchConfig, database: Option[String]): Connection =
              super.buildDorisJdbcConnection(config.copy(password = "test-password"), database)
          }
      }
      val failure = intercept[SQLException](writer.write())
      assert(failure.getErrorCode == 1045 && failure.getSQLState == "28000")
      assertTaskState(config, DorisRecordStatus.FAILURE.id, writtenRows = -1L)
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 2L)
    }
  }

  test("empty Connector input keeps successful empty transaction query behavior") {
    val (database, table) = createTransactionTable("it_doris_empty_2pc")
    val config = batchConfig(database, table, enable2PC = true)
    withMetadataContext() { context =>
      var runtimePrefix = ""
      val result = trackedWriter(config, twoColumnSource(Seq.empty), context) { writeConfig =>
        runtimePrefix = writeConfig.labelPrefix
      }.write()
      assert(result.success && result.rowCount == 0L)
      assert(queryColumn(s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '${runtimePrefix}-%'")(
        _.getString("TransactionStatus")).isEmpty)
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
      assertTaskState(config, DorisRecordStatus.SUCCESS.id, writtenRows = 0L)
    }
  }

  for (sameTable <- Seq(false, true)) {
    test(s"shared configured prefix leaves other transactions untouched when sameTable=$sameTable") {
      val (database, table) = createTransactionTable("it_doris_scope")
      val victim = if (sameTable) table else createTransactionTable("it_doris_victim")._2
      val prefix = TestDataGenerator.uniqueName("pista_shared")
      val config = batchConfig(database, table, labelPrefix = prefix, enable2PC = true)
      val otherRun = DorisTransactionScope.create(prefix)
      val victimConfig = config.copy(table = victim)
      val ids = ListBuffer.empty[String]
      try {
        Seq(prefix + "_external", otherRun.labelPrefix + "-pending", prefix + "_aborted").foreach { label =>
          ids += createPreCommittedTransaction(victimConfig.copy(labelPrefix = label), "901,pending\n")
        }
        abortExact(victimConfig, ids.last)
        var runtimePrefix = ""
        assert(trackedWriter(config, twoColumnSource(1 to 2)) { writeConfig =>
          runtimePrefix = writeConfig.labelPrefix
        }.write().success)
        assert(ids.map(transactionState(victimConfig, _)).toSeq == Seq("PRECOMMITTED", "PRECOMMITTED", "ABORTED"))
        val statuses = queryColumn(s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '$runtimePrefix-%'")(
          _.getString("TransactionStatus"))
        assert(statuses.nonEmpty && statuses.forall(_ == "VISIBLE"))
        assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 2L)
        if (!sameTable) assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$victim`") == 0L)
      } finally ids.foreach { id =>
        if (transactionState(victimConfig, id) == "PRECOMMITTED") abortExact(victimConfig, id)
      }
    }
  }

  for (failureMode <- Seq("write", "timeout")) {
    test(s"$failureMode cleanup aborts only the current invocation") {
      val (database, table) = createTransactionTable("it_doris_owned_cleanup")
      val (_, victim) = createTransactionTable("it_doris_foreign_cleanup")
      val config = batchConfig(database, table, enable2PC = true)
      val victimConfig = config.copy(table = victim, labelPrefix = config.labelPrefix + "_other")
      val foreignId = createPreCommittedTransaction(victimConfig, "901,foreign\n")
      var ownedId = ""
      try {
        val writer = trackedWriter(config, twoColumnSource(1 to 2), visibilityWaitMs = 100) { writeConfig =>
          ownedId = createPreCommittedTransaction(
            writeConfig.copy(labelPrefix = writeConfig.labelPrefix + "-pending"), "902,owned\n")
          if (failureMode == "write") throw new IllegalStateException("injected connector failure")
        }
        intercept[Exception](writer.write())
        assert(ownedId.nonEmpty && transactionState(config, ownedId) == "ABORTED")
        assert(transactionState(victimConfig, foreignId) == "PRECOMMITTED")
        assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == (if (failureMode == "write") 0L else 2L))
        assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$victim`") == 0L)
      } finally {
        abortExact(victimConfig, foreignId)
        if (ownedId.nonEmpty && transactionState(config, ownedId) == "PRECOMMITTED") abortExact(config, ownedId)
      }
    }
  }

  test("a Spark input task retry keeps the runtime namespace and produces one visible copy") {
    val (database, table) = createTransactionTable("it_doris_scope_retry")
    val config = batchConfig(database, table, labelPrefix = "prefix.*_%'\\" + "\u4e2d" * 500, enable2PC = true)
    val input = twoColumnSource(1 to 4)
    val retried = input.rdd.mapPartitions { rows =>
      val task = TaskContext.get()
      if (task.partitionId() == 0 && task.attemptNumber() == 0)
        throw new IllegalStateException("injected first input attempt failure")
      rows.map(row => Row(row.getInt(0), s"attempt-${task.attemptNumber()}"))
    }
    var runtimePrefix = ""
    val result = trackedWriter(config, spark.createDataFrame(retried, input.schema)) { writeConfig =>
      runtimePrefix = writeConfig.labelPrefix
    }.write()
    assert(result.success && result.rowCount == 4L)
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table` WHERE value = 'attempt-1'") > 0L)
    assert(queryLong(s"SELECT COUNT(DISTINCT id) FROM `$database`.`$table`") == 4L)
    val labels = queryColumn(s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '$runtimePrefix-%'")(
      _.getString("Label"))
    assert(labels.nonEmpty && labels.forall(label => label.startsWith(runtimePrefix + "-") && label.length <= 128))
  }

  private def trackedWriter(
    config: DorisBatchConfig, source: DataFrame,
    context: DorisConcurrentContext = DorisConcurrentContext(None, None), visibilityWaitMs: Int = 30000
  )(beforeWrite: DorisBatchConfig => Unit): DorisConcurrentWriter =
    new DorisConcurrentWriter(config, context, source, System.currentTimeMillis()) {
      override protected def createConnectorWriter(
        writeConfig: DorisBatchConfig, sourceDf: DataFrame, timestamp: Long): SparkDorisConnectorWriter = {
        beforeWrite(writeConfig)
        super.createConnectorWriter(writeConfig, sourceDf, timestamp)
      }
      override protected def createTransactionCoordinator(): DorisTransactionCoordinator =
        new DorisTransactionCoordinator {
          override def validateAllVisible(config: DorisBatchConfig, scope: DorisTransactionScope, maxWaitMs: Int): Boolean =
            super.validateAllVisible(config, scope, visibilityWaitMs)
        }
    }

  private def transactionState(config: DorisBatchConfig, txnId: String): String =
    queryColumn(s"SHOW TRANSACTION FROM `${config.database}` WHERE id = ${txnId.toLong}")(
      _.getString("TransactionStatus")).head

  private def abortExact(config: DorisBatchConfig, txnId: String): Unit = {
    val coordinator = new DorisTransactionCoordinator {
      def abort(): Boolean = send2pcAbort(txnId, config)
    }
    assert(coordinator.abort())
    assert(transactionState(config, txnId) == "ABORTED")
  }

  private def createTransactionTable(prefix: String): (String, String) =
    createDorisTable(
      prefix,
      """(id INT, value VARCHAR(128))
        |DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 2
        |PROPERTIES ("replication_num" = "2")""".stripMargin)

  private def createPreCommittedTransaction(
    config: DorisBatchConfig,
    payload: String
  ): String = {
    val beEndpoint = config.benodes.split(",").head.trim
    val connection = new URL(
      s"http://$beEndpoint/api/${config.database}/${config.table}/_stream_load")
      .openConnection().asInstanceOf[HttpURLConnection]
    val bytes = payload.getBytes(StandardCharsets.UTF_8)

    try {
      connection.setRequestMethod("PUT")
      connection.setDoOutput(true)
      connection.setConnectTimeout(10000)
      connection.setReadTimeout(30000)
      connection.setFixedLengthStreamingMode(bytes.length)
      connection.setRequestProperty("Authorization",
        s"Basic ${Base64.getEncoder.encodeToString("root:".getBytes(StandardCharsets.UTF_8))}")
      connection.setRequestProperty("Expect", "100-continue")
      connection.setRequestProperty("Content-Type", "text/plain")
      connection.setRequestProperty("label", config.labelPrefix)
      connection.setRequestProperty("two_phase_commit", "true")
      connection.setRequestProperty("format", "csv")
      connection.setRequestProperty("column_separator", ",")
      connection.setRequestProperty("columns", "id,value")

      val output = connection.getOutputStream
      try output.write(bytes)
      finally output.close()

      val responseCode = connection.getResponseCode
      val responseStream =
        if (responseCode >= 400) connection.getErrorStream else connection.getInputStream
      val body = if (responseStream == null) "" else {
        try scala.io.Source.fromInputStream(responseStream, "UTF-8").mkString
        finally responseStream.close()
      }
      assert(responseCode == 200, s"Stream Load 2PC HTTP $responseCode: $body")

      val json = new ObjectMapper().readTree(body)
      assert(Option(json.get("Status")).exists(_.asText().equalsIgnoreCase("Success")),
        s"Stream Load 2PC failure: $body")
      Option(json.get("TxnId")).map(_.asText()).filter(_.nonEmpty).getOrElse(
        fail(s"Stream Load 2PC did not return TxnId: $body"))
    } finally connection.disconnect()
  }
}
