package com.pista.spark.sql.connector.doris.batch

import org.apache.spark.sql.DataFrame

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

/** A fresh, bounded label namespace for one writer invocation, independent of metadata identity. */
final class DorisTransactionScope private (val labelPrefix: String) {
  // Connector 25.2.0 appends -stageId-taskAttemptId-partitionId-time to this prefix.
  def ownsLabel(label: String): Boolean =
    label.startsWith(labelPrefix + "-") && label.length > labelPrefix.length + 1
}

object DorisTransactionScope {
  def create(configuredPrefix: String): DorisTransactionScope = {
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(configuredPrefix.getBytes(StandardCharsets.UTF_8))
      .take(8).map("%02x".format(_)).mkString
    // 55 ASCII characters leave room for the connector suffix under the 128-character baseline.
    new DorisTransactionScope(s"pista_${digest}_${UUID.randomUUID().toString.replace("-", "")}")
  }
}

/**
 * Transaction Coordinator Trait
 *
 * Provide a unified transaction management interface for Doris 2PC (two-phase commit).
 * Core Capability:
 * - Configure transaction timeout (deprecated, Doris 3.x does not support)
 * - PreCommitted Transactions (PreCommit monitoring)
 * - validate transaction status (confirm final state after job completion)
 * - abort owned transactions after write failure or timeout
 *
 * Design goal:
 * - Decouple transaction management from specific write logic
 * - coordinates Spark Connector, Stream Load, and Broker Load transactions
 * - Provide unified 2PC transaction visibility
 *
 */
trait TransactionCoordinator {

  /**
   * Configure transaction timeout time
   *
   * @deprecated transaction_timeout_second Does not exist in Doris 3.x, this method is ineffective.
   * @param sourceDf Source DataFrame (used for estimating data volume)
   * @param config   Batch write configuration
   * @return Transaction Timeout (seconds)
   */
  @deprecated("transaction_timeout_second is not supported in Doris 3.x", "2.4")
  def configureTimeout(sourceDf: DataFrame, config: DorisBatchConfig): Int

  /**
   * Query transactions in the committed-but-unpersisted state
   *
   * Return only the current invocation's transactions in the PRECOMMITTED state.
   * Used for monitoring and debugging to confirm that data has been written but not yet visible.
   *
   * @param config Batch write configuration
   * @return PreCommitted Transaction List (txn_id, label, db, table)
   */
  def queryPreCommittedTxns(config: DorisBatchConfig, scope: DorisTransactionScope): Seq[PreCommittedTxn]

  /**
   * Validate transaction final state
   *
   * After Spark Job completes, call to confirm whether the transaction has been successfully committed.
   *
   * @param txnId transaction ID
   * @param config Batch write configuration
   * @return true=task committed (COMMITTED/VISIBLE),false=task failed or rolled back
   */
  def validateTransactionState(txnId: String, config: DorisBatchConfig): Boolean

  /**
   * Validate the final state of transactions by prefixing them with Label
   *
   * Spark Doris Connector exposes no txnId, query with a label prefix instead.
   * Query records where the label matches the prefix in SHOW TRANSACTION,
   * Return true when no transaction remains PRECOMMITTED; terminal transactions are COMMITTED or VISIBLE.
   *
   * @param scope Current invocation's label namespace; a configured prefix or table URL is not ownership.
   * @return true=all committed transactions, false=exists uncommitted transaction
   */
  def validateTransactionByLabel(config: DorisBatchConfig, scope: DorisTransactionScope): Boolean

  /**
   * Validate the final state of transactions with a Label prefix (historically aware)
   *
   * @param config     Batch write configuration
   * @param hasHistory Whether metadata contains a prior transaction record. If true and SHOW TRANSACTION returns no record,
   *                   Conservatively return false to avoid overwriting due to expired transactions, preserving the conservative approach of cleaning up transactions.
   * @return true=the validation succeeds,false=false
   */
  def validateTransactionByLabel(config: DorisBatchConfig, scope: DorisTransactionScope, hasHistory: Boolean): Boolean

  /**
   * Poll until all transactions reach the VISIBLE state
   *
   * After write succeeds, invokes to ensure the transaction is both COMMITTED and VISIBLE (physically visible).
   * Poll the SHOW TRANSACTION query for transaction status until all transactions are VISIBLE or timeout.
   *
   * @param config    Batch write configuration
   * @param maxWaitMs Maximum wait time (milliseconds), defaults to 30000.
   * @return true=all transactions are VISIBLE, false=time-out or presence of failed transactions
   */
  def validateAllVisible(config: DorisBatchConfig, scope: DorisTransactionScope, maxWaitMs: Int = 30000): Boolean

  /**
   * Abort only PRECOMMITTED transactions owned by the current invocation.
   *
   * Through HTTP PUT _stream_load_2pc API send the abort request through the HTTP PUT _stream_load_2pc API. abort request.
   * Used for cleanup after write failure or timeout. Never cleans a shared configured prefix.
   *
   * @param config Batch write configuration
   * @return Number of transactions aborted successfully
   */
  def abortByLabel(config: DorisBatchConfig, scope: DorisTransactionScope): Int

  /**
   * abort expired transactions
   *
   * @deprecated Replace with abortByLabel which is more precise and unaffected by time calculations.
   * @param config Batch write configuration
   * @return Number of transactions aborted successfully
   */
  @deprecated("Use abortByLabel instead", "2.4")
  def abortStaleTxns(config: DorisBatchConfig, scope: DorisTransactionScope): Int
}

/**
 * pre-submitted transaction records
 *
 * @param txnId transaction ID
 * @param label   Stream Load Label
 * @param database_name Database name
 * @param table   Column name
 * @param startTime Transaction start time
 */
case class PreCommittedTxn(
  txnId:     String,
  label:     String,
  db:        String,
  table:     String,
  startTime: Long
)
