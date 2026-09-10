package com.pista.spark.errors

import org.scalatest.funsuite.AnyFunSuite

/**
 * PistaErrorMessageLoader Unit Test
 *
 */
class PistaErrorMessageLoaderSuite extends AnyFunSuite {

  test("getMessage should correctly load Pista error code") {
    val message = PistaErrorMessageLoader.getMessage(
      "PISTA_CONFIG_MISSING_REQUIRED",
      Map("configKey" -> "\"spark.pista.sqlFile\""))

    assert(message.contains("[PISTA_CONFIG_MISSING_REQUIRED]"))
    assert(message.contains("spark.pista.sqlFile"))
    assert(message.contains("Required configuration"))
  }

  test("getMessage should correctly replace multiple parameters") {
    val message = PistaErrorMessageLoader.getMessage(
      "PISTA_CONFIG_INVALID_VALUE",
      Map(
        "configKey" -> "\"spark.pista.errorPolicy\"",
        "value" -> "\"invalid\"",
        "expected" -> "fail-fast or continue-on-error"))

    assert(message.contains("[PISTA_CONFIG_INVALID_VALUE]"))
    assert(message.contains("spark.pista.errorPolicy"))
    assert(message.contains("invalid"))
    assert(message.contains("fail-fast or continue-on-error"))
  }

  test("getMessage should correctly load Spark error code") {
    val message = PistaErrorMessageLoader.getMessage(
      "INTERNAL_ERROR",
      Map("message" -> "test error"))

    assert(message.contains("[INTERNAL_ERROR]"))
    assert(message.contains("test error"))
  }

  test("getMessage should handle a missing error code") {
    val message = PistaErrorMessageLoader.getMessage(
      "NON_EXISTENT_ERROR_CODE",
      Map("param1" -> "value1"))

    assert(message.contains("[NON_EXISTENT_ERROR_CODE]"))
    assert(message.contains("param1"))
    assert(message.contains("value1"))
  }

  test("getMessage should handle empty parameters") {
    val message = PistaErrorMessageLoader.getMessage(
      "PISTA_CHECKPOINT_NOT_INITIALIZED",
      Map.empty)

    assert(message.contains("[PISTA_CHECKPOINT_NOT_INITIALIZED]"))
    assert(message.contains("MaterializationManager is not initialized"))
  }

  test("getSqlState should return the correct SQL State") {
    val sqlState = PistaErrorMessageLoader.getSqlState("PISTA_CONFIG_MISSING_REQUIRED")
    assert(sqlState.contains("42000"))
  }

  test("getSqlState should handle error codes without SQL State") {
    val sqlState = PistaErrorMessageLoader.getSqlState("NON_EXISTENT_ERROR_CODE")
    assert(sqlState.isEmpty)
  }

  test("all Pista error codes should be loaded") {
    val pistaErrorCodes = Seq(
      "PISTA_CONFIG_MISSING_REQUIRED",
      "PISTA_CONFIG_INVALID_VALUE",
      "PISTA_INVALID_SQL_FILE",
      "PISTA_SQL_PARSE_ERROR",
      "PISTA_WRITER_NOT_FOUND",
      "PISTA_WRITER_VALIDATION_FAILED",
      "PISTA_UNSUPPORTED_OUTPUT_FORMAT",
      "PISTA_READER_NOT_FOUND",
      "PISTA_READER_CONNECTION_FAILED",
      "PISTA_PROCESSOR_CLASS_NOT_FOUND",
      "PISTA_PROCESSOR_EXECUTION_FAILED",
      "PISTA_CHECKPOINT_NOT_INITIALIZED",
      "PISTA_CHECKPOINT_SAVE_FAILED",
      "PISTA_CHECKPOINT_RESTORE_FAILED",
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      "PISTA_UDF_INVALID_ARGUMENT_TYPE"
    )

    pistaErrorCodes.foreach { errorCode =>
      val message = PistaErrorMessageLoader.getMessage(errorCode, Map.empty)
      assert(message.contains(s"[$errorCode]"), s"Error code $errorCode should be loaded")
    }
  }

  test("part of Spark errors should be loaded Spark error code") {
    val sparkErrorCodes = Seq(
      "INTERNAL_ERROR",
      "UNSUPPORTED_FEATURE",
      "INVALID_PARAMETER_VALUE"
    )

    sparkErrorCodes.foreach { errorCode =>
      val message = PistaErrorMessageLoader.getMessage(errorCode, Map.empty)
      assert(message.contains(s"[$errorCode]"), s"Spark error code $errorCode should be loaded")
    }
  }
}
