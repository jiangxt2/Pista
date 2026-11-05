package com.pista.spark.sql.connector.doris.batch

import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, ResultSet, SQLException, Statement}
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

  private val scope = DorisTransactionScope.create("pista")

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
    ): Seq[Map[String, String]] = rows.map(row =>
      if (row.contains("label")) row else row + ("label" -> (scope.labelPrefix + "-test")))
  }

  private def jdbcStub[T](kind: Class[T])(call: (String, Array[AnyRef]) => AnyRef): T =
    kind.cast(Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[_]](kind),
      new InvocationHandler {
        override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
          call(method.getName, args)
      }))

  Seq("empty", "execute", "read-status").foreach { stage =>
    test(s"JDBC $stage preserves empty results or the primary query failure and closes resources") {
      val original = new SQLException("query rejected", "08006")
      val secondary = new SQLException("connection close failed", "08006")
      val closed = ListBuffer.empty[String]
      val result = jdbcStub(classOf[ResultSet]) { (method, args) => method match {
        case "next" => java.lang.Boolean.valueOf(stage == "read-status")
        case "getString" if args(0) == "TransactionStatus" => throw original
        case "getString" => "value"
        case "close" => closed += "result"; null
        case other => throw new UnsupportedOperationException(other)
      }}
      val statement = jdbcStub(classOf[Statement]) { (method, _) => method match {
        case "executeQuery" if stage == "execute" => throw original
        case "executeQuery" => result
        case "close" => closed += "statement"; null
        case other => throw new UnsupportedOperationException(other)
      }}
      val connection = jdbcStub(classOf[Connection]) { (method, _) => method match {
        case "createStatement" => statement
        case "close" =>
          closed += "connection"
          if (stage != "empty") throw secondary
          null
        case other => throw new UnsupportedOperationException(other)
      }}
      val coordinator = new DorisTransactionCoordinator {
        override protected def buildDorisJdbcConnection(
          config: DorisBatchConfig, database: Option[String]): Connection = connection
      }
      if (stage == "empty") assert(coordinator.validateAllVisible(config, scope, maxWaitMs = 1000))
      else {
        val failure = intercept[SQLException](coordinator.validateAllVisible(config, scope, maxWaitMs = 1000))
        assert(failure eq original)
        assert(failure.getSuppressed.toSeq.contains(secondary))
      }
      val expected = stage match {
        case "execute" => Seq("statement", "connection")
        case _ => Seq("result", "statement", "connection")
      }
      assert(closed.toSeq == expected)
    }
  }

  test("runtime namespaces bound arbitrary configured prefixes and leave room for connector labels") {
    val configured = "prefix.*_%'\\" + "\u4e2d" * 500
    val first = DorisTransactionScope.create(configured)
    val second = DorisTransactionScope.create(configured)
    assert(first.labelPrefix.matches("pista_[0-9a-f]{16}_[0-9a-f]{32}"))
    assert(first.labelPrefix.take(23) == second.labelPrefix.take(23))
    assert(first.labelPrefix != second.labelPrefix)
    val longestSuffix = s"-${Int.MinValue}-${Long.MinValue}-${Int.MinValue}-${Long.MinValue}"
    assert((first.labelPrefix + longestSuffix).length <= 128)
    assert(first.ownsLabel(first.labelPrefix + longestSuffix))
    assert(!first.ownsLabel(first.labelPrefix))
    assert(!first.ownsLabel(first.labelPrefix + "-"))
    assert(!first.ownsLabel(first.labelPrefix + "x-1"))
    assert(!first.ownsLabel(second.labelPrefix + "-1"))
  }

  test("all label operations filter literal ownership before validation or exact-ID abort") {
    val aborted = ListBuffer.empty[String]
    val queries = ListBuffer.empty[String]
    var pending = true
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql: String, config: DorisBatchConfig): Seq[Map[String, String]] = {
        queries += sql
        Seq(
          Map("label" -> (scope.labelPrefix + "-own"), "txn_id" -> "101",
            "status" -> (if (pending) "PRECOMMITTED" else "VISIBLE")),
          Map("label" -> (scope.labelPrefix + "x-foreign"), "txn_id" -> "102", "status" -> "PRECOMMITTED"),
          Map("label" -> "pista_other", "txn_id" -> "103", "status" -> "ABORTED"))
      }
      override protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = {
        aborted += txnId
        true
      }
    }
    assert(coordinator.abortByLabel(config, scope) == 1)
    assert(aborted.toSeq == Seq("101"))
    pending = false
    assert(coordinator.queryTransactionStatuses(config, scope) == Seq("VISIBLE"))
    assert(coordinator.validateTransactionByLabel(config, scope))
    assert(coordinator.validateAllVisible(config, scope))
    assert(queries.forall(_.contains(s"'${scope.labelPrefix}-%'")))
  }

  // ==================== validateTransactionByLabel with hasHistory ====================

  test("No metadata history and no transaction records indicate validation success. Meta History and transaction records absence indicate validation success.") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.validateTransactionByLabel(config, scope, hasHistory = false))
  }

  test("there is no transactional record but there is historical metadata. Meta conservatively reject when there is historical metadata but no transactional records") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(!coordinator.validateTransactionByLabel(config, scope, hasHistory = true))
  }

  test("validation succeeds when metadata history exists and every transaction is COMMITTED") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED")
    ))
    assert(coordinator.validateTransactionByLabel(config, scope, hasHistory = true))
  }

  test("there is Meta historical and all transactions VISIBLE it validates through.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "VISIBLE")
    ))
    assert(coordinator.validateTransactionByLabel(config, scope, hasHistory = true))
  }

  test("Existence PRECOMMITTED Fail validation when a PRECOMMITTED transaction is present.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED"),
      Map("status" -> "PRECOMMITTED")
    ))
    assert(!coordinator.validateTransactionByLabel(config, scope, hasHistory = true))
  }

  test("existent PREPARE Fail validation when transaction is present.") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "PREPARE")
    ))
    assert(!coordinator.validateTransactionByLabel(config, scope, hasHistory = false))
  }

  test("exists ABORTED transactions validate failure") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "ABORTED")
    ))
    assert(!coordinator.validateTransactionByLabel(config, scope, hasHistory = false))
  }

  test("exist unknown state transactions to validate failure") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "UNKNOWN")
    ))
    assert(!coordinator.validateTransactionByLabel(config, scope, hasHistory = false))
  }

  // ==================== validateAllVisible ====================

  test("validateAllVisible return true when no transaction records exist true") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.validateAllVisible(config, scope, maxWaitMs = 1000))
  }

  test("validateAllVisible returns true when all transactions are VISIBLE") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "VISIBLE"),
      Map("status" -> "VISIBLE")
    ))
    assert(coordinator.validateAllVisible(config, scope, maxWaitMs = 1000))
  }

  test("validateAllVisible returns true when PREPARE becomes VISIBLE after polling") {
    val states = scala.collection.mutable.Queue(
      Seq(Map("status" -> "PREPARE", "txn_id" -> "1", "label" -> (scope.labelPrefix + "-lbl1"))),
      Seq(Map("status" -> "VISIBLE", "txn_id" -> "1", "label" -> (scope.labelPrefix + "-lbl1")))
    )
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = states.dequeue()
    }
    assert(coordinator.validateAllVisible(config, scope, maxWaitMs = 10000))
  }

  test("validateAllVisible exists as ABORTED returns false prematurely") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED"),
      Map("status" -> "ABORTED")
    ))
    assert(!coordinator.validateAllVisible(config, scope, maxWaitMs = 1000))
  }

  test("validateAllVisible Timeout (Always COMMITTED) returns false") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "COMMITTED")
    ))
    assert(!coordinator.validateAllVisible(config, scope, maxWaitMs = 100))
  }

  test("validateAllVisible Timeout (always PRECOMMITTED) returns false") {
    val coordinator = mockCoordinator(Seq(
      Map("status" -> "PRECOMMITTED")
    ))
    assert(!coordinator.validateAllVisible(config, scope, maxWaitMs = 100))
  }

  // ==================== abortByLabel ====================

  test("abortByLabel does not return 0 for a PRECOMMITTED transaction") {
    val coordinator = mockCoordinator(Seq.empty)
    assert(coordinator.abortByLabel(config, scope) == 0)
  }

  test("abortByLabel has PRECOMMITTED and successfully aborts return the count") {
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "101", "label" -> (scope.labelPrefix + "-lbl1"))
      )
      override protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = true
    }
    assert(coordinator.abortByLabel(config, scope) == 1)
  }

  test("abortByLabel Partial Failures Return Success Count") {
    val coordinator = new DorisTransactionCoordinator {
      override private[batch] def queryShowTransaction(
        sql:    String,
        config: DorisBatchConfig
      ): Seq[Map[String, String]] = Seq(
        Map("status" -> "PRECOMMITTED", "txn_id" -> "101", "label" -> (scope.labelPrefix + "-lbl1")),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "102", "label" -> (scope.labelPrefix + "-lbl2"))
      )
      override protected def send2pcAbort(txnId: String, config: DorisBatchConfig): Boolean = txnId == "101"
    }
    assert(coordinator.abortByLabel(config, scope) == 1)
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
          "label" -> (scope.labelPrefix + "-old-label"), "txn_begin_ts" -> (now - 7200000L).toString),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "recent",
          "label" -> (scope.labelPrefix + "-recent-label"), "txn_begin_ts" -> now.toString),
        Map("status" -> "VISIBLE", "txn_id" -> "visible",
          "label" -> (scope.labelPrefix + "-visible-label"), "txn_begin_ts" -> (now - 7200000L).toString))
      override protected def send2pcAbort(
        txnId: String,
        config: DorisBatchConfig
      ): Boolean = {
        aborted += txnId
        true
      }
    }

    assert(coordinator.abortStaleTxns(config, scope) == 1)
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
          "label" -> (scope.labelPrefix + "-label-101"), "txn_begin_ts" -> old.toString),
        Map("status" -> "PRECOMMITTED", "txn_id" -> "102",
          "label" -> (scope.labelPrefix + "-label-102"), "txn_begin_ts" -> old.toString))
      override protected def send2pcAbort(
        txnId: String,
        config: DorisBatchConfig
      ): Boolean = txnId == "101"
    }

    assert(coordinator.abortStaleTxns(config, scope) == 1)
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
