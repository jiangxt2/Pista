package com.pista.spark.errors

import org.scalatest.funsuite.AnyFunSuite

/**
 * PistaUDFException Unit Test
 *
 */
class PistaUDFExceptionSuite extends AnyFunSuite {

  test("PistaUDFException Should Be Correctly Constructed") {
    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map(
        "udfName" -> "test_udf",
        "expected" -> "2",
        "actual" -> "3"))

    assert(exception.getErrorClass == "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH")
    assert(exception.getMessage.contains("test_udf"))
    assert(exception.getMessage.contains("2"))
    assert(exception.getMessage.contains("3"))
  }

  test("PistaUDFException should inherit RuntimeException") {
    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map.empty)

    assert(exception.isInstanceOf[RuntimeException])
  }

  test("PistaUDFException should implement SparkThrowable") {
    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map.empty)

    assert(exception.isInstanceOf[org.apache.spark.SparkThrowable])
  }

  test("PistaUDFException should return the correct messageParameters") {
    val params = Map(
      "udfName" -> "test_udf",
      "expected" -> "2",
      "actual" -> "3")

    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      params)

    val returnedParams = exception.getMessageParameters
    assert(returnedParams.get("udfName") == "test_udf")
    assert(returnedParams.get("expected") == "2")
    assert(returnedParams.get("actual") == "3")
  }

  test("PistaUDFException support an exception with a cause. cause Constructor construction") {
    val cause = new RuntimeException("Original error")
    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map.empty,
      cause)

    assert(exception.getCause == cause)
    assert(exception.getCause.getMessage == "Original error")
  }

  test("PistaUDFException supports construction without a cause") {
    val exception = new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map.empty)

    assert(exception.getCause == null)
  }
}
