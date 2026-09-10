package com.pista.spark.sql.connector.doris.batch

import org.scalatest.funsuite.AnyFunSuite

/**
 *DorisTableSwapManager Unit Test*
 *
 * Test content:
 * - tempTableName Naming Rule
 * - Column Structure Validation (Symmetry against DorisPartitionManager)
 */
class DorisTableSwapManagerSuite extends AnyFunSuite {

  private def minimalConfig(table: String = "mytable") = DorisBatchConfig(
    fenodes    = "127.0.0.1:8030",
    database   = "mydb",
    table      = table,
    password   = ""
  )

  test("tempTableName naming rule is correct") {
    val mgr = new DorisTableSwapManager(minimalConfig(), 1234567890L)
    assert(mgr.tempTableName == "tmp_mytable_1234567890")
  }

  test("tempTableName contains special characters and retains them") {
    val mgr = new DorisTableSwapManager(minimalConfig("user_log_2026"), 999L)
    assert(mgr.tempTableName == "tmp_user_log_2026_999")
  }

  test("DorisTableSwapManager and DorisPartitionManager methods are symmetric") {
    val swapMgr = new DorisTableSwapManager(minimalConfig(), 0L)
    val partMgr = new DorisPartitionManager(minimalConfig(), 0L)

    // Both have three core methods: prepare → transform → clean
    assert(swapMgr.getClass.getDeclaredMethods.exists(_.getName == "createTempTable"))
    assert(swapMgr.getClass.getDeclaredMethods.exists(_.getName == "replaceTable"))
    assert(swapMgr.getClass.getDeclaredMethods.exists(_.getName == "cleanupTempTable"))

    assert(partMgr.getClass.getDeclaredMethods.exists(_.getName == "prepareTempPartitions"))
    assert(partMgr.getClass.getDeclaredMethods.exists(_.getName == "replacePartitions"))
    assert(partMgr.getClass.getDeclaredMethods.exists(_.getName == "cleanupTempPartitions"))
  }
}
