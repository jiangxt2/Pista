package com.pista.spark.sql.connector.doris.batch

import org.scalatest.funsuite.AnyFunSuite

/**
 *DorisPartitionManager Unit Test*
 *
 * Test content:
 * - parseRangeToValues:SHOW PARTITIONS Range column parsing converts to VALUES clause
 * - formatValuepartition key value is formatted as VALUES literal
 */
class DorisPartitionManagerSuite extends AnyFunSuite {

  private val manager = new DorisPartitionManager(
    DorisBatchConfig(
      fenodes  = "127.0.0.1:8030",
      database = "db",
      table    = "tbl",
      password = ""
    ),
    0L
  )

  // ==================== parseRangeToValues ====================

  test("BIGINT Type Normal Range Parsing") {
    val rangeStr = "[types: [BIGINT]; keys: [-9223372036854775808]; ..types: [BIGINT]; keys: [3000000]; )"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(\"-9223372036854775808\"), (\"3000000\"))")
  }

  test("DATE Type Normal Range Parsing") {
    val rangeStr = "[types: [DATE]; keys: [2026-04-07]; ..types: [DATE]; keys: [2026-04-08]; )"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(\"2026-04-07\"), (\"2026-04-08\"))")
  }

  test("VARCHAR type containing ] characters Range parsed") {
    val rangeStr = "[types: [VARCHAR]; keys: [a]b]; ..types: [VARCHAR]; keys: [c]d]; )"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(\"a]b\"), (\"c]d\"))")
  }

  test("values contain multiple ] character") {
    val rangeStr = "[types: [VARCHAR]; keys: [x]]y]]z]; ..types: [VARCHAR]; keys: [a]]b]; ]"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(\"x]]y]]z\"), (\"a]]b\")]")
  }

  test("MINVALUE/MAXVALUE boundary values are not quoted") {
    val rangeStr = "[types: [BIGINT]; keys: [MINVALUE]; ..types: [BIGINT]; keys: [MAXVALUE]; )"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(MINVALUE), (MAXVALUE))")
  }

  test("inclusive boundary ] resolves correctly") {
    val rangeStr = "[types: [DATE]; keys: [2026-04-07]; ..types: [DATE]; keys: [2026-04-08]; ]"
    val result = manager.parseRangeToValues(rangeStr)
    assert(result == "[(\"2026-04-07\"), (\"2026-04-08\")]")
  }

  test("missing keys are thrown as RuntimeException. keys throw an exception RuntimeException") {
    val rangeStr = "[types: [BIGINT]; nokeys: [100]; )"
    val ex = intercept[RuntimeException] {
      manager.parseRangeToValues(rangeStr)
    }
    assert(ex.getMessage.contains("Cannot find 'keys:'"))
  }

  test("Exception Format: Missing ] Closing Throws RuntimeException") {
    val rangeStr = "[types: [BIGINT]; keys: [100; ..types: [BIGINT]; keys: [200]; )"
    val ex = intercept[RuntimeException] {
      manager.parseRangeToValues(rangeStr)
    }
    assert(ex.getMessage.contains("Cannot find ']'"))
  }

  test("Exception Format: Throw RuntimeException for unknown boundary character") {
    val rangeStr = "[types: [BIGINT]; keys: [100]; ..types: [BIGINT]; keys: [200]; }"
    val ex = intercept[RuntimeException] {
      manager.parseRangeToValues(rangeStr)
    }
    assert(ex.getMessage.contains("Unknown boundary"))
  }
}
