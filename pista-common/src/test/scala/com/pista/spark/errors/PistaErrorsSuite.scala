package com.pista.spark.errors

import org.scalatest.funsuite.AnyFunSuite

/**
 * PistaErrors unit tests
 *
 */
class PistaErrorsSuite extends AnyFunSuite {

  test("missingRequiredConfigError should generate the correct exception") {
    val error = PistaErrors.missingRequiredConfigError("spark.pista.sqlFile_")

    assert(error.isInstanceOf[PistaConfigException])
    assert(error.getErrorClass == "PISTA_CONFIG_MISSING_REQUIRED")
    assert(error.getMessage.contains("spark.pista.sqlFile_"))
  }

  test("invalidConfigValueError should generate the correct exception") {
    val error = PistaErrors.invalidConfigValueError(
      "spark.pista.errorPolicy",
      "invalid",
      "fail-fast or continue-on-error"
    )

    assert(error.isInstanceOf[PistaConfigException])
    assert(error.getErrorClass == "PISTA_CONFIG_INVALID_VALUE")
    assert(error.getMessage.contains("invalid"))
    assert(error.getMessage.contains("fail-fast or continue-on-error"))
  }

  test("invalidSqlFileError should generate the correct exception") {
    val error = PistaErrors.invalidSqlFileError("/tmp/test.sql")

    assert(error.isInstanceOf[PistaSQLFileException])
    assert(error.getErrorClass == "PISTA_INVALID_SQL_FILE")
    assert(error.getMessage.contains("/tmp/test.sql"))
  }

  test("writerNotFoundError should generate the correct exception") {
    val error = PistaErrors.writerNotFoundError("mysql", Seq("doris", "clickhouse"))

    assert(error.isInstanceOf[PistaWriterException])
    assert(error.getErrorClass == "PISTA_WRITER_NOT_FOUND")
    assert(error.getMessage.contains("mysql"))
    assert(error.getMessage.contains("doris, clickhouse"))
  }

  test("readerNotFoundError should generate the correct exception") {
    val error = PistaErrors.readerNotFoundError("oracle", Seq("doris", "clickhouse"))

    assert(error.isInstanceOf[PistaReaderException])
    assert(error.getErrorClass == "PISTA_READER_NOT_FOUND")
    assert(error.getMessage.contains("oracle"))
  }

  test("processorClassNotFoundError should generate the correct exception") {
    val error = PistaErrors.processorClassNotFoundError("com.example.MyProcessor")

    assert(error.isInstanceOf[PistaProcessorException])
    assert(error.getErrorClass == "PISTA_PROCESSOR_CLASS_NOT_FOUND")
    assert(error.getMessage.contains("com.example.MyProcessor"))
  }

  test("checkpointNotInitializedError should generate the correct exception") {
    val error = PistaErrors.checkpointNotInitializedError()

    assert(error.isInstanceOf[PistaCheckpointException])
    assert(error.getErrorClass == "PISTA_CHECKPOINT_NOT_INITIALIZED")
  }

  test("all errors should have errorClass") {
    val errors = Seq(
      PistaErrors.missingRequiredConfigError("test"),
      PistaErrors.invalidConfigValueError("test", "value", "expected"),
      PistaErrors.invalidSqlFileError("test.sql"),
      PistaErrors.writerNotFoundError("test", Seq.empty),
      PistaErrors.readerNotFoundError("test", Seq.empty),
      PistaErrors.processorClassNotFoundError("test"),
      PistaErrors.checkpointNotInitializedError()
    )

    errors.foreach { error =>
      assert(error.getErrorClass != null)
      assert(error.getErrorClass.startsWith("PISTA_"))
    }
  }
}
