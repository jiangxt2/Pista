package com.pista.spark.sql.connector.doris

import com.fasterxml.jackson.databind.ObjectMapper
import com.pista.spark.sql.connector.doris.batch._
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.test.util.TestDataGenerator

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.util.Base64

class DorisTransactionIT extends DorisITSupport {

  test("Doris Connector 2PC transaction visible after commit VISIBLE data visible") {
    val (database, table) = createTransactionTable("it_doris_2pc_commit")
    val config = batchConfig(
      database,
      table,
      labelPrefix = TestDataGenerator.uniqueName("pista_it_2pc_commit"),
      enable2PC = true)

    val result = new DorisConcurrentWriter(
      config,
      DorisConcurrentContext(clusterName = None, metaManager = None),
      twoColumnSource(1 to 20),
      System.currentTimeMillis()).write()

    assert(result.success)
    assert(result.rowCount == 20L)
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 20L)

    val statuses = queryColumn(
      s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '${config.labelPrefix}%'")(_.getString("TransactionStatus"))
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

    val txnId = createPreCommittedTransaction(config, "901,precommitted\n")
    val coordinator = new DorisTransactionCoordinator()
    assert(coordinator.queryTransactionStatuses(config).contains("PRECOMMITTED"))
    assert(coordinator.abortByLabel(config) == 1)
    assert(coordinator.queryTransactionStatuses(config).contains("ABORTED"),
      s"transaction $txnId has not converged ABORTED")
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
  }

  test("rejected transaction query cannot validate an invisible PRECOMMITTED transaction") {
    val (database, table) = createTransactionTable("it_doris_query_failure")
    val config = batchConfig(database, table, enable2PC = true)
    val coordinator = new DorisTransactionCoordinator {
      def abortExact(txnId: String): Boolean = send2pcAbort(txnId, config)
    }
    val txnId = createPreCommittedTransaction(config, "901,pending\n")
    def state: Seq[String] = queryColumn(
      s"SHOW TRANSACTION FROM `$database` WHERE id = ${txnId.toLong}")(_.getString("TransactionStatus"))
    try {
      assert(state == Seq("PRECOMMITTED"))
      val failure = intercept[SQLException] {
        coordinator.validateAllVisible(config.copy(password = "test-password"), maxWaitMs = 1000)
      }
      assert(failure.getErrorCode == 1045 && failure.getSQLState == "28000")
      assert(state == Seq("PRECOMMITTED"))
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
    } finally {
      assert(coordinator.abortExact(txnId))
      assert(state == Seq("ABORTED"))
    }
  }

  test("transaction preparation authentication failure records FAILURE after RUNNING") {
    val (database, table) = createTransactionTable("it_doris_prepare_failure")
    val config = batchConfig(database, table, enable2PC = true)
      .copy(password = "test-password")
    withMetadataContext() { context =>
      val failure = intercept[SQLException] {
        new DorisConcurrentWriter(config, context, twoColumnSource(Seq(1)), System.currentTimeMillis()).write()
      }
      assert(failure.getErrorCode == 1045 && failure.getSQLState == "28000")
      assertTaskState(config, DorisRecordStatus.FAILURE.id, writtenRows = -1L)
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
    }
  }

  test("empty Connector input keeps successful empty transaction query behavior") {
    val (database, table) = createTransactionTable("it_doris_empty_2pc")
    val config = batchConfig(database, table, enable2PC = true)
    withMetadataContext() { context =>
      val result = new DorisConcurrentWriter(
        config, context, twoColumnSource(Seq.empty), System.currentTimeMillis()).write()
      assert(result.success && result.rowCount == 0L)
      assert(queryColumn(s"SHOW TRANSACTION FROM `$database` WHERE label LIKE '${config.labelPrefix}%'")(
        _.getString("TransactionStatus")).isEmpty)
      assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 0L)
      assertTaskState(config, DorisRecordStatus.SUCCESS.id, writtenRows = 0L)
    }
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
