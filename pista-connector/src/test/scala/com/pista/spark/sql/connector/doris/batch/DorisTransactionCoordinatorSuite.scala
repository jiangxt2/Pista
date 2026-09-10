package com.pista.spark.sql.connector.doris.batch

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ListBuffer

/**
 *DorisTransactionCoordinator Unit Test*
 *
 * Test content:
 * - validateTransactionByLabelhaving label and optionally having history hasHistory parameters) various branches
 * - No Records + Has/No History
 * - COMMITTED/VISIBLE / PRECOMMITTED / ABORTED / UNKNOWN STATE
 */
class DorisTransactionCoordinatorSuite extends AnyFunSuite {

  private val config = DorisBatchConfig(
    fenodes  = "127.0.0.1:8030",
    database = "testdb",
    table    = "testtbl",
    password = ""
  )

  /** Mock coordinator: Overwrite queryShowTransaction to return specified rows */
  private def mockCoordinator(rows: Seq[Map[String, String]]) = new DorisTransactionCoordinator {
    override private[batch] def queryShowTransaction(
      sql:    String,
      config: DorisBatchConfig
    ): Seq[Map[String, String]] = rows
  }

  // ==================== validateTransactionByLabel with hasHistory ====================

  test("No metadata history and no transaction records indicate validation success. Meta History and transaction records absence indicate validation success.") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.validateTransactionByLabel(config, hasHistory = false))
  }

  test("there is no transactional record but there is historical metadata. Meta conservatively reject when there is historical metadata but no transactional records") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(!coordinator.validateTransactionByLabel(config, hasHistory = true))
  }

  test("validation succeeds when metadata history exists and every transaction is COMMITTED") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED")
    ))
    assert(coordinator.validateTransactionByLabel(config, hasHistory = true))
  }

  test("there is Meta historical and all transactions VISIBLE it validates through.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "VISIBLE")
    ))
    assert(coordinator.validateTransactionByLabel(config, hasHistory = true))
  }

  test("Existence PRECOMMITTED Fail validation when a PRECOMMITTED transaction is present.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED"),
      Map("status" -> "PRECOMMITTED")
    ))
    assert(!coordinator.validateTransactionByLabel(config, hasHistory = true))
  }

  test("existent PREPARE Fail validation when transaction is present.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "PREPARE")
    ))
    assert(!coordinator.validateTransactionByLabel(config, hasHistory = false))
  }

  test("exists ABORTED transactions validate failure") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "ABORTED")
    ))
    assert(!coordinator.validateTransactionByLabel(config, hasHistory = false))
  }

  test("exist unknown state transactions to validate failure") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "UNKNOWN")
    ))
    assert(!coordinator.validateTransactionByLabel(config, hasHistory = false))
  }

  // ==================== validateAllVisible ====================

  test("validateAllVisible return true when no transaction records exist true") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.validateAllVisible(config, maxWaitMs = 1000))
  }

  test("validateAllVisible returns true when all transactions are VISIBLE") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "VISIBLE"),
      Map("status" -> "VISIBLE")
    ))
    assert(coordinator.validateAllVisible(config, maxWaitMs = 1000))
  }

  test("validateAllVisible returns true when PREPARE becomes VISIBLE after polling") {
    val states = scala.collection.mutable.Queue(
      Seq(Map("status" -> "PREPARE", "txn_id" -> "1", "label" -> "lbl1")),
      Seq(Map("status" -> "VISIBLE", "txn_id" -> "1", "label" -> "lbl1"))
    )
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = states.dequeue()
    }
    assert(coordinator.validateAllVisible(config, maxWaitMs = 10000))
  }

  test("validateAllVisible exists as ABORTED returns false prematurely") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED"),
      Map("status" -> "ABORTED")
    ))
    assert(!coordinator.validateAllVisible(config, maxWaitMs = 1000))
  }

  test("validateAllVisible Timeout (Always COMMITTED) returns false") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED")
    ))
    assert(!coordinator.validateAllVisible(config, maxWaitMs = 100))
  }

  test("validateAllVisible Timeout (always PRECOMMITTED) returns false") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "PRECOMMITTED")
    ))
    assert(!coordinator.validateAllVisible(config, maxWaitMs = 100))
  }

  // ==================== abortByLabel ====================

  test("abortByLabel does not return 0 for a PRECOMMITTED transaction") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.abortByLabel(config) == 0)
  }

  test("abortByLabel has PRECOMMITTED and successfully aborts return the count") {
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "101", "label" -> "lbl1")
      )
      override protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = true
    }
    assert(coordinator.abortByLabel(config) == 1)
  }

  test("abortByLabel Partial Failures Return Success Count") {
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "101", "label" -> "lbl1"),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "102", "label" -> "lbl2")
      )
      override protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = txnId == "101"
    }
    assert(coordinator.abortByLabel(config) == 1)
  }

  test("abortStaleTxns Aborts transactions that are more than one hour in the PRECOMMITTED state") {
    val now = System.currentTimeMillis()
    val aborted = ListBuffer.empty[String]
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql: String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "old",
          "label" -> "old-label", "txn_begin_ts" -> (now - 7200000L).toString),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "recent",
          "label" -> "recent-label", "txn_begin_ts" -> now.toString),
        Map("status" -> "VISIBLE", "txn_id" -> "visible",
          "label" -> "visible-label", "txn_begin_ts" -> (now - 7200000L).toString))
      override protected def send2pcAbort(
        txnId: String,
        config: DorisBatchConfig
      ): Boolean = {
        aborted += txnId
        true
      }
    }

    assert(coordinator.abortStaleTxns(config) == 1)
    assert(aborted.toSeq == Seq("old"))
  }

  test("abortStaleTxns tolerates a single abort return success count after tolerating a single abort failure") {
    val old = System.currentTimeMillis() - 7200000L
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql: String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "101",
          "label" -> "label-101", "txn_begin_ts" -> old.toString),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "102",
          "label" -> "label-102", "txn_begin_ts" -> old.toString))
      override protected def send2pcAbort(
        txnId: String,
        config: DorisBatchConfig
      ): Boolean = txnId == "101"
    }

    assert(coordinator.abortStaleTxns(config) == 1)
  }

  test("2PC HTTP endpoint selects based on autoRedirect and nodes benodes selection") {
    val coordinator = new DorisTransactionCoordinator()
    val directConfig = config.copy(
      fenodes = "fe:8030",
      autoRedirect = false,
      benodes = "be1:8040,be2:8040")

    assert(coordinator.resolve2pcEndpoint(directConfig) == "be1:8040")
    assert(coordinator.resolve2pcEndpoint(
      directConfig.copy(autoRedirect = true)) == "fe:8030")
    assert(coordinator.resolve2pcEndpoint(
      directConfig.copy(benodes = "")) == "fe:8030")
  }
}
