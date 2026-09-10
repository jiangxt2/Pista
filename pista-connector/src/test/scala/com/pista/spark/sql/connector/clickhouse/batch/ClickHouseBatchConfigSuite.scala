package com.pista.spark.sql.connector.clickhouse.batch

import org.scalatest.funsuite.AnyFunSuite

class ClickHouseBatchConfigSuite extends AnyFunSuite {

  test("ClickHouseBatchConfig Default Values Correct") {
    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://127.0.0.1:8123/test_db"
    )
    assert(config.batchSize === 200000L)
    assert(config.outputPartitions === 10)
    assert(config.overwrite === false)
    assert(config.overwriteMode === "on_cluster")
    assert(config.convertNullToDefault === false)
    assert(config.partitionDate === "")
    assert(config.partitionColumn === "")
    assert(config.skipLocalDelete === false)
    assert(config.clearDataMaxAttempts === 30)
    assert(config.clearDataIntervalMs === 10000L)
    assert(config.maxBackupSwitches === 1)
  }

  test("ClickHouseBatchConfig options transmission") {
    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://127.0.0.1:8123/test_db",
      options = Map("user" -> "admin", "password" -> "secret")
    )
    assert(config.options("user") === "admin")
    assert(config.options("password") === "secret")
  }
}
