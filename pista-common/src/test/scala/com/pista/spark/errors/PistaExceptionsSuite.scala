package com.pista.spark.errors

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pista Exception Unit Tests
 *
 */
class PistaExceptionsSuite extends AnyFunSuite {

  // ==================== PistaConfigException Exception Test ====================

  test("PistaConfigException should inherit IllegalArgumentException") {
    val exception = new PistaConfigException(
      "PISTA_CONFIG_MISSING_REQUIRED",
      Map("configKey" -> "\"test\""))

    assert(exception.isInstanceOf[IllegalArgumentException])
  }

  test("PistaConfigException should implement SparkThrowable") {
    val exception = new PistaConfigException(
      "PISTA_CONFIG_MISSING_REQUIRED",
      Map.empty)

    assert(exception.isInstanceOf[org.apache.spark.SparkThrowable])
  }

  test("PistaConfigException should return the correct errorClass") {
    val exception = new PistaConfigException(
      "PISTA_CONFIG_MISSING_REQUIRED",
      Map.empty)

    assert(exception.getErrorClass == "PISTA_CONFIG_MISSING_REQUIRED")
  }

  test("PistaConfigException should return the correct messageParameters") {
    val params = Map("configKey" -> "\"test\"")
    val exception = new PistaConfigException(
      "PISTA_CONFIG_MISSING_REQUIRED",
      params)

    val returnedParams = exception.getMessageParameters
    assert(returnedParams.get("configKey") == "\"test\"")
  }

  // ==================== PistaSQLFileException Test ====================

  test("PistaSQLFileException should inherit FileNotFoundException") {
    val exception = new PistaSQLFileException(
      "PISTA_INVALID_SQL_FILE",
      Map("filePath" -> "/tmp/test.sql"))

    assert(exception.isInstanceOf[java.io.FileNotFoundException])
  }

  test("PistaSQLFileException should implement SparkThrowable") {
    val exception = new PistaSQLFileException(
      "PISTA_INVALID_SQL_FILE",
      Map.empty)

    assert(exception.isInstanceOf[org.apache.spark.SparkThrowable])
  }

  test("PistaSQLFileException should contain file path information") {
    val exception = new PistaSQLFileException(
      "PISTA_INVALID_SQL_FILE",
      Map("filePath" -> "/tmp/test.sql"))

    assert(exception.getMessage.contains("/tmp/test.sql"))
  }

  // ==================== PistaWriterException Test ====================

  test("PistaWriterException should inherit RuntimeException") {
    val exception = new PistaWriterException(
      "PISTA_WRITER_NOT_FOUND",
      Map.empty)

    assert(exception.isInstanceOf[RuntimeException])
  }

  test("PistaWriterException supports construction with a cause") {
    val cause = new RuntimeException("Original error")
    val exception = new PistaWriterException(
      "PISTA_WRITER_NOT_FOUND",
      Map.empty,
      cause)

    assert(exception.getCause == cause)
  }

  test("PistaWriterException supports construction without a cause") {
    val exception = new PistaWriterException(
      "PISTA_WRITER_NOT_FOUND",
      Map.empty)

    assert(exception.getCause == null)
  }

  // ==================== PistaReaderException Test ====================

  test("PistaReaderException should inherit RuntimeException") {
    val exception = new PistaReaderException(
      "PISTA_READER_NOT_FOUND",
      Map.empty)

    assert(exception.isInstanceOf[RuntimeException])
  }

  test("PistaReaderException supports construction with a cause") {
    val cause = new RuntimeException("Connection failed")
    val exception = new PistaReaderException(
      "PISTA_READER_CONNECTION_FAILED",
      Map("source" -> "mysql", "reason" -> "timeout"),
      cause)

    assert(exception.getCause == cause)
    assert(exception.getMessage.contains("mysql"))
  }

  // ==================== PistaProcessorException Test ====================

  test("PistaProcessorException should inherit RuntimeException") {
    val exception = new PistaProcessorException(
      "PISTA_PROCESSOR_CLASS_NOT_FOUND",
      Map.empty)

    assert(exception.isInstanceOf[RuntimeException])
  }

  test("PistaProcessorException supports construction with a cause") {
    val cause = new ClassNotFoundException("MyProcessor")
    val exception = new PistaProcessorException(
      "PISTA_PROCESSOR_CLASS_NOT_FOUND",
      Map("className" -> "MyProcessor"),
      cause)

    assert(exception.getCause == cause)
    assert(exception.getMessage.contains("MyProcessor"))
  }

  // ==================== PistaCheckpointException Test ====================

  test("PistaCheckpointException should inherit RuntimeException") {
    val exception = new PistaCheckpointException(
      "PISTA_CHECKPOINT_NOT_INITIALIZED",
      Map.empty)

    assert(exception.isInstanceOf[RuntimeException])
  }

  test("PistaCheckpointException supports construction with a cause") {
    val cause = new java.io.IOException("Disk full")
    val exception = new PistaCheckpointException(
      "PISTA_CHECKPOINT_SAVE_FAILED",
      Map("path" -> "/tmp/checkpoint", "reason" -> "disk full"),
      cause)

    assert(exception.getCause == cause)
    assert(exception.getMessage.contains("/tmp/checkpoint"))
  }

  // ==================== General Test ====================

  test("all exception classes should implement SparkThrowable") {
    val exceptions = Seq(
      new PistaConfigException("TEST", Map.empty),
      new PistaSQLFileException("TEST", Map.empty),
      new PistaWriterException("TEST", Map.empty),
      new PistaReaderException("TEST", Map.empty),
      new PistaProcessorException("TEST", Map.empty),
      new PistaCheckpointException("TEST", Map.empty)
    )

    exceptions.foreach { exception =>
      assert(exception.isInstanceOf[org.apache.spark.SparkThrowable],
        s"${exception.getClass.getSimpleName} should implement SparkThrowable")
    }
  }

  test("all exception classes should return the correct errorClass") {
    val errorClass = "TEST_ERROR_CLASS"
    val exceptions = Seq(
      new PistaConfigException(errorClass, Map.empty),
      new PistaSQLFileException(errorClass, Map.empty),
      new PistaWriterException(errorClass, Map.empty),
      new PistaReaderException(errorClass, Map.empty),
      new PistaProcessorException(errorClass, Map.empty),
      new PistaCheckpointException(errorClass, Map.empty)
    )

    exceptions.foreach { exception =>
      assert(exception.getErrorClass == errorClass,
        s"${exception.getClass.getSimpleName} should return correct errorClass")
    }
  }

  test("all exception classes should return the correct messageParameters") {
    val params = Map("key1" -> "value1", "key2" -> "value2")
    val exceptions = Seq(
      new PistaConfigException("TEST", params),
      new PistaSQLFileException("TEST", params),
      new PistaWriterException("TEST", params),
      new PistaReaderException("TEST", params),
      new PistaProcessorException("TEST", params),
      new PistaCheckpointException("TEST", params)
    )

    exceptions.foreach { exception =>
      val returnedParams = exception.getMessageParameters
      assert(returnedParams.get("key1") == "value1",
        s"${exception.getClass.getSimpleName} should return correct messageParameters")
      assert(returnedParams.get("key2") == "value2",
        s"${exception.getClass.getSimpleName} should return correct messageParameters")
    }
  }
}
