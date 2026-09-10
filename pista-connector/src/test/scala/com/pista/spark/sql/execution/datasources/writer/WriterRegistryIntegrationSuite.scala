package com.pista.spark.sql.execution.datasources.writer

import com.pista.spark.errors.PistaWriterException
import org.apache.spark.internal.Logging
import org.scalatest.funsuite.AnyFunSuite

/**
 * WriterRegistry test
 *
 * Validate ServiceLoader automatic discovery and registration functionality
 *
 */
class WriterRegistryIntegrationSuite extends AnyFunSuite with Logging {

  test("Unsupported formats should throw an exception and list available formats.") {
    val config = OutputConfig(
      path = None,
      format = "unsupported_format",
      mode = "append",
      partitionBy = Seq.empty,
      options = Map.empty
    )

    val exception = intercept[PistaWriterException] {
      WriterRegistry.write(null, config)
    }

    // Verify that the error message includes the format name and a list of available writer columns.
    assert(exception.getMessage.contains("Writer for format"))
    assert(exception.getMessage.contains("unsupported_format"))

    // Verify that the available formats are listed.
    assert(exception.getMessage.contains("doris"))
    assert(exception.getMessage.contains("clickhouse"))

    logInfo(s"✓ Error message correct: ${exception.getMessage}")
  }

  test("ServiceLoader should automatically discover all Writer") {
    // Get the list of supported formats by triggering an unsupported format exception
    val config = OutputConfig(
      path = None,
      format = "test_format_not_exist",
      mode = "append",
      partitionBy = Seq.empty,
      options = Map.empty
    )

    val exception = intercept[PistaWriterException] {
      WriterRegistry.write(null, config)
    }

    val message = exception.getMessage

    WriterRegistry.availableFormats.foreach(logInfo(_))

    // Verify that at least 2 Writers have been discovered.
    assert(message.contains("doris"), "should discover DorisWriter")
    assert(message.contains("clickhouse"), "should discover ClickHouseWriter")

    logInfo(s"✓ Found Writer: doris, clickhouse")
  }
}
