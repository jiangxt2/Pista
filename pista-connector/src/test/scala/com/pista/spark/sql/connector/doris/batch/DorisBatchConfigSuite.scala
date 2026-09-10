package com.pista.spark.sql.connector.doris.batch

import org.scalatest.funsuite.AnyFunSuite

class DorisBatchConfigSuite extends AnyFunSuite {

  test("DorisBatchConfig Default Values Correct") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = ""
    )
    assert(config.user === "root")
    assert(config.password === "")
    assert(config.writeMode === WriteMode.SPARK_CONNECTOR)
    assert(config.labelPrefix === "pista")
    assert(config.outputPartitions === 50)
    assert(config.batchSize === 500000)
    assert(config.dataFormat === "csv")
    assert(config.enable2PC === false)
    assert(config.autoRedirect === true)
    assert(config.feQueryPort === 9030)
    assert(config.overwrite === false)
  }

  test("overwrite=true and partitionDate isPartitionOverwrite=true when partitionDate is not null isPartitionOverwrite=true") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = "",
      overwrite = true,
      partitionDate = "20260407"
    )
    assert(config.isPartitionOverwrite === true)
  }

  test("overwrite=true and partitionDate is null when isPartitionOverwrite=false") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = "",
      overwrite = true
    )
    assert(config.isPartitionOverwrite === false)
  }

  test("overwrite=false always disables isPartitionOverwrite") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = "",
      partitionDate = "20260407"
    )
    assert(config.isPartitionOverwrite === false)
  }

  test("WriteMode character parsing") {
    assert(WriteMode.fromString("spark_connector") === WriteMode.SPARK_CONNECTOR)
    assert(WriteMode.fromString("stream_load") === WriteMode.STREAM_LOAD)
    assert(WriteMode.fromString("broker_load") === WriteMode.BROKER_LOAD)
  }

  test("WriteMode UNKNOWN throws exception") {
    val ex = intercept[IllegalArgumentException] {
      WriteMode.fromString("unknown_mode")
    }
    assert(ex.getMessage.contains("Unknown writeMode"))
  }

  test("targetPartitions null partitionDate returns Nil") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = "",
      partitionDate = ""
    )
    assert(config.targetPartitions === Nil)
  }

  test("targetPartitions correctly derives partition names") {
    val config = DorisBatchConfig(
      fenodes = "127.0.0.1:8030",
      database = "test_db",
      table = "test_table",
      password = "",
      partitionDate = "20260401,20260402"
    )
    assert(config.targetPartitions === Seq("p20260401", "p20260402"))
  }
}
