package com.pista.spark.errors

import org.scalatest.funsuite.AnyFunSuite

/**
 * PistaUDFErrors unit tests
 *
 */
class PistaUDFErrorsSuite extends AnyFunSuite {

  test("udfArgumentLengthMismatchError should generate the correct exception") {
    val error = PistaUDFErrors.udfArgumentLengthMismatchError(
      "test_udf",
      2,
      3)

    assert(error.isInstanceOf[PistaUDFException])
    assert(error.getErrorClass == "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH")
    assert(error.getMessage.contains("test_udf"))
    assert(error.getMessage.contains("2"))
    assert(error.getMessage.contains("3"))
  }

  test("udfArgumentLengthMismatchError the test should include correct parameters") {
    val error = PistaUDFErrors.udfArgumentLengthMismatchError(
      "my_function",
      5,
      3)

    val params = error.getMessageParameters
    assert(params.get("udfName") == "my_function")
    assert(params.get("expected") == "5")
    assert(params.get("actual") == "3")
  }

  test("udfInvalidArgumentTypeError should generate the correct exception") {
    val error = PistaUDFErrors.udfInvalidArgumentTypeError(
      "test_udf",
      1,
      "StringType",
      "IntegerType")

    assert(error.isInstanceOf[PistaUDFException])
    assert(error.getErrorClass == "PISTA_UDF_INVALID_ARGUMENT_TYPE")
    assert(error.getMessage.contains("test_udf"))
    assert(error.getMessage.contains("1"))
    assert(error.getMessage.contains("StringType"))
    assert(error.getMessage.contains("IntegerType"))
  }

  test("udfInvalidArgumentTypeError should include correct parameters") {
    val error = PistaUDFErrors.udfInvalidArgumentTypeError(
      "my_function",
      2,
      "DoubleType",
      "LongType")

    val params = error.getMessageParameters
    assert(params.get("udfName") == "my_function")
    assert(params.get("argIndex") == "2")
    assert(params.get("actualType") == "DoubleType")
    assert(params.get("expectedType") == "LongType")
  }

  test("All UDF errors should have the correct errorClass") {
    val errors = Seq(
      PistaUDFErrors.udfArgumentLengthMismatchError("test", 1, 2),
      PistaUDFErrors.udfInvalidArgumentTypeError("test", 0, "String", "Int"),
      PistaUDFErrors.functionInvalidInputError("test", "bad input"),
      PistaUDFErrors.functionResourceLimitError("test", "bytes", 10, 11),
      PistaUDFErrors.functionBinaryFormatError("test", "bad envelope"),
      PistaUDFErrors.functionCatalogConflictError("test", "pista", "other"),
      PistaUDFErrors.functionInstallationError("failed")
    )

    errors.foreach { error =>
      assert(error.getErrorClass != null)
      assert(error.getErrorClass.startsWith("PISTA_UDF_") ||
        error.getErrorClass.startsWith("PISTA_FUNCTION_"))
    }
  }

  test("All UDF errors should be implemented as SparkThrowable") {
    val errors = Seq(
      PistaUDFErrors.udfArgumentLengthMismatchError("test", 1, 2),
      PistaUDFErrors.udfInvalidArgumentTypeError("test", 0, "String", "Int"),
      PistaUDFErrors.functionInvalidInputError("test", "bad input"),
      PistaUDFErrors.functionResourceLimitError("test", "bytes", 10, 11),
      PistaUDFErrors.functionBinaryFormatError("test", "bad envelope"),
      PistaUDFErrors.functionCatalogConflictError("test", "pista", "other"),
      PistaUDFErrors.functionInstallationError("failed")
    )

    errors.foreach { error =>
      assert(error.isInstanceOf[org.apache.spark.SparkThrowable])
    }
  }
}
