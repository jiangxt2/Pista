package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.sql.connector.doris.DorisJdbcSupport
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import scala.util.{Try, Using}

/**
 * Doris 2PC transaction coordinator implementation
 *
 * SHOW TRANSACTION command is used to query transaction status (rather than the non-existent information_schema.transactions).
 * Implement transactional state monitoring, timeout configuration, and dead transaction cleanup.
 *
 * Core Capability:
 * - Adaptive timeout computation (based on number of partitions + estimation of data volume)
 * - Query transactions in PRECOMMITTED state (data written but not visible)
 * - By Label verifying the final state of transactions with a prefix (Connector does not expose txnId)Connector do not expose txnId alternative solution)
 * - Clean timeout transactions via HTTP PUT _stream_load_2pc
 *
 */
class DorisTransactionCoordinator extends TransactionCoordinator with DorisJdbcSupport with Logging {

  override def configureTimeout(sourceDf: DataFrame, config: DorisBatchConfig): Int = {
    // Deprecated: transaction_timeout_second is not available in Doris 3.x.
    // Return default value of 3600s (1 hour), which will not be used actually.
    3600
  }

  override def queryPreCommittedTxns(config: DorisBatchConfig, scope: DorisTransactionScope): Seq[PreCommittedTxn] = {
    queryOwnedTransactions(config, scope).filter(_("status") == "PRECOMMITTED").map { row =>
      PreCommittedTxn(
        txnId = row("txn_id"),
        label = row("label"),
        db = config.database,
        table = config.table,
        startTime = row.get("txn_begin_ts").map(_.toLong).getOrElse(0L)
      )
    }
  }

  override def validateTransactionState(txnId: String, config: DorisBatchConfig): Boolean = {
    val sql = s"SHOW TRANSACTION FROM `${config.database}` WHERE id = ${txnId.toLong}"

    try {
      val rows = queryShowTransaction(sql, config)
      rows.headOption match {
        case Some(row) =>
          val status = row("status")
          val isSuccess = status == "COMMITTED" || status == "VISIBLE"
          logInfo(s"[TransactionCoordinator] Transaction $txnId status: $status, success=$isSuccess")
          isSuccess
        case None =>
          logWarning(s"[TransactionCoordinator] Transaction $txnId not found")
          false
      }
    } catch {
      case e: Exception =>
        logError(s"[TransactionCoordinator] Transaction validation failed with ${e.getClass.getSimpleName}")
        false
    }
  }

  /**
   * Validate the final state of transactions prefixed with Label.
   *
   * Consider success only by "PRECOMMITTED" status is insufficient: transactions may also be ABORTED.
   * Therefore, here requires:
   * 1. If a transaction is found, all states are COMMITTED/VISIBLE.
   * 2. Disallow PRECOMMITTED/ABORTED/PREPARE states.
   * 3. previous transactions, need to be combined with whether there is historical metadata. Meta historical judgment:
   *    - existent Meta Historical entries without transactions (non-first run): conservative error (transactions may have expired and their status is unknown)
   *    - No Meta History (first run): Considered as passed validation:
   */
  override def validateTransactionByLabel(config: DorisBatchConfig, scope: DorisTransactionScope): Boolean =
    validateTransactionByLabel(config, scope, hasHistory = false)

  /**
   * Validate the final state of transactions prefixed with Label (with historical awareness).
   *
   * @param hasHistory Whether metadata contains a prior transaction record. If true and SHOW TRANSACTION returns no record,
   *                   Mark transactions as expired to return false conservatively to avoid duplicate writes.
   */
  override def validateTransactionByLabel(
    config: DorisBatchConfig, scope: DorisTransactionScope, hasHistory: Boolean
  ): Boolean = {
    val rows = queryOwnedTransactions(config, scope)
    if (rows.isEmpty) {
      if (hasHistory) {
        logWarning(s"[TransactionCoordinator] No transactions found for label prefix '${scope.labelPrefix}', " +
          "but meta history exists. Previous transaction may have been cleaned up. " +
          "Refusing to proceed to avoid potential duplicate data.")
        false
      } else {
        logInfo(s"[TransactionCoordinator] No prior transactions found for label prefix '${scope.labelPrefix}', " +
          "treating as clean state (first run or transactions already finalized)")
        true
      }
    } else {
      val statuses = rows.map(_("status")).distinct
      val hasPending = statuses.contains("PRECOMMITTED") || statuses.contains("PREPARE")
      val hasFailure = statuses.contains("ABORTED")
      val allSuccess = statuses.forall(status => status == "COMMITTED" || status == "VISIBLE")

      if (hasPending) {
        logWarning(s"[TransactionCoordinator] Found pending transactions (PREPARE/PRECOMMITTED) " +
          s"for label prefix '${scope.labelPrefix}'")
        false
      } else if (hasFailure || !allSuccess) {
        logWarning(s"[TransactionCoordinator] Transaction validation failed for label prefix " +
          s"'${scope.labelPrefix}', statuses=${statuses.mkString(",")}")
        false
      } else {
        logInfo(s"[TransactionCoordinator] Transaction validation passed for label prefix " +
          s"'${scope.labelPrefix}', statuses=${statuses.mkString(",")}")
        true
      }
    }
  }

  override def validateAllVisible(
    config: DorisBatchConfig, scope: DorisTransactionScope, maxWaitMs: Int = 30000
  ): Boolean = {
    val startMs = System.currentTimeMillis()
    val intervalMs = 2000

    while (System.currentTimeMillis() - startMs < maxWaitMs) {
      val rows = queryOwnedTransactions(config, scope)

      if (rows.isEmpty) {
        logInfo("[TransactionCoordinator] No transactions found, treating as clean")
        return true
      }

      val statuses = rows.map(_("status")).distinct
      val hasPending = statuses.exists(s => s == "PRECOMMITTED" || s == "PREPARE")
      val hasFailure = statuses.contains("ABORTED")
      val allVisible = statuses.forall(s => s == "VISIBLE")

      if (hasFailure) {
        logError(s"[TransactionCoordinator] Found ABORTED transactions, statuses=${statuses.mkString(",")}")
        return false
      }

      if (!hasPending && allVisible) {
        logInfo(s"[TransactionCoordinator] All transactions visible, statuses=${statuses.mkString(",")}")
        return true
      }

      if (hasPending || statuses.contains("COMMITTED")) {
        logInfo(s"[TransactionCoordinator] Waiting for transactions to become visible, " +
          s"current statuses=${statuses.mkString(",")}, " +
          s"elapsed=${(System.currentTimeMillis() - startMs) / 1000}s")
      }

      Thread.sleep(intervalMs)
    }

    logError(s"[TransactionCoordinator] Timeout after ${maxWaitMs}ms, transactions not all visible")
    false
  }

  override def abortByLabel(config: DorisBatchConfig, scope: DorisTransactionScope): Int = {
    val txns = queryPreCommittedTxns(config, scope)
    if (txns.isEmpty) {
      logInfo("[TransactionCoordinator] No pre-committed transactions to abort")
      0
    } else {
      logWarning(s"[TransactionCoordinator] Aborting ${txns.size} pre-committed transactions")
      var abortedCount = 0
      txns.foreach { txn =>
        try {
          if (send2pcAbort(txn.txnId, config)) {
            logInfo(s"[TransactionCoordinator] Aborted txn ${txn.txnId} (label=${txn.label})")
            abortedCount += 1
          }
        } catch {
          case e: Exception =>
            logError(s"[TransactionCoordinator] Transaction abort failed with ${e.getClass.getSimpleName}")
        }
      }
      logInfo(s"[TransactionCoordinator] Aborted $abortedCount/${txns.size} transactions")
      abortedCount
    }
  }

  /**
   * Query the status list of all transactions prefixed with the specified Label
   *
   * @return A list of transaction status strings (e.g., Seq("VISIBLE", "COMMITTED")), returning an empty list when no transaction exists.
   */
  def queryTransactionStatuses(config: DorisBatchConfig, scope: DorisTransactionScope): Seq[String] =
    queryOwnedTransactions(config, scope).map(_("status"))

  override def abortStaleTxns(config: DorisBatchConfig, scope: DorisTransactionScope): Int = {
    val preCommittedTxns = queryPreCommittedTxns(config, scope)
    val now = System.currentTimeMillis()
    // Deprecated: use the fixed one-hour timeout (3600 seconds).
    val timeoutMs = 3600 * 1000L

    val staleTxns = preCommittedTxns.filter(txn => (now - txn.startTime) > timeoutMs)

    if (staleTxns.isEmpty) {
      logInfo("[TransactionCoordinator] No stale transactions to abort")
      0
    } else {
      logWarning(s"[TransactionCoordinator] Found ${staleTxns.size} stale transactions, aborting...")

      var abortedCount = 0
      staleTxns.foreach { txn =>
        try {
          val aborted = send2pcAbort(txn.txnId, config)
          if (aborted) {
            logInfo(s"[TransactionCoordinator] Aborted txn ${txn.txnId} (label=${txn.label})")
            abortedCount += 1
          }
        } catch {
          case e: Exception =>
            logError(s"[TransactionCoordinator] Transaction abort failed with ${e.getClass.getSimpleName}")
        }
      }

      logInfo(s"[TransactionCoordinator] Aborted $abortedCount/${staleTxns.size} stale transactions")
      abortedCount
    }
  }

  protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = {
    val endpoint = resolve2pcEndpoint(config)
    val url = s"http://$endpoint/api/${config.database}/${config.table}/_stream_load_2pc"

    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("PUT")
      conn.setDoOutput(true)
      conn.setRequestProperty("Authorization",
        s"Basic ${java.util.Base64.getEncoder.encodeToString(s"${config.user}:${config.password}".getBytes)}")
      conn.setRequestProperty("txn_id", txnId)
      conn.setRequestProperty("txn_operation", "abort")
      conn.setConnectTimeout(10000)
      conn.setReadTimeout(30000)

      val responseCode = conn.getResponseCode
      val stream = if (responseCode >= 400) conn.getErrorStream else conn.getInputStream
      val body = Option(stream).map { is =>
        Using.resource(new BufferedReader(new InputStreamReader(is))) { reader =>
          Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
        }
      }.getOrElse("")

      val success = responseCode == 200 && {
        Try {
          val node = new ObjectMapper().readTree(body)
          Option(node.get("status")).exists(_.asText().equalsIgnoreCase("success"))
        }.getOrElse(false)
      }
      if (!success)
        logWarning(s"[TransactionCoordinator] Abort response: code=$responseCode, body=$body")
      success
    } catch {
      case e: Exception =>
        logError(s"[TransactionCoordinator] 2PC abort failed with ${e.getClass.getSimpleName}")
        false
    }
  }

  /**
   * Docker/NAT Scenarios where FE automatically redirects is disabled, the mapped BE HTTP address must be used.
   * Otherwise, the BE address returned by FE is not reachable from the host JVM.
   */
  private[batch] def resolve2pcEndpoint(config: DorisBatchConfig): String = {
    val candidates =
      if (!config.autoRedirect && config.benodes.trim.nonEmpty) config.benodes
      else config.fenodes
    candidates.split(",").head.trim
  }

  /**
   * Execute the SHOW TRANSACTION command and return the structured result.
   *
   * SHOW TRANSACTION is a MySQL style command that does not support PreparedStatement parameterization,
   * Callers use a generated label namespace or a validated numeric transaction ID.
   *
   * Return column (Doris 3.x): TransactionId, Label, Coordinator, TransactionStatus,
   * LoadJobSourceType, PrepareTime, PreCommitTime, CommitTime, FinishTime, Reason,
   * ErrorReplicasCount, ListenerId, TimeoutMs, VisibleTime
   */
  private[batch] def queryShowTransaction(
    sql: String,
    config: DorisBatchConfig
  ): Seq[Map[String, String]] =
    Using.resource(buildDorisJdbcConnection(config, Some(config.database))) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        Using.resource(stmt.executeQuery(sql)) { rs =>
          Iterator.continually(rs.next()).takeWhile(identity).map { _ =>
            Map(
              "txn_id"       -> Option(rs.getString("TransactionId")).getOrElse(""),
              "label"        -> Option(rs.getString("Label")).getOrElse(""),
              "status"       -> Option(rs.getString("TransactionStatus")).getOrElse(""),
              "txn_begin_ts" -> Option(rs.getTimestamp("PrepareTime")).map(_.getTime.toString).getOrElse("0")
            )
          }.toList
        }
      }
    }

  private def queryOwnedTransactions(
    config: DorisBatchConfig, scope: DorisTransactionScope
  ): Seq[Map[String, String]] = {
    // This SHOW path uses Java regex matching in Doris 3.0.6.2. The generated prefix
    // contains only ASCII letters, digits and underscores; still enforce literal ownership.
    val sql = s"SHOW TRANSACTION FROM `${config.database}` WHERE label LIKE '${scope.labelPrefix}-%'"
    queryShowTransaction(sql, config).filter(row => row.get("label").exists(scope.ownsLabel))
  }
}
