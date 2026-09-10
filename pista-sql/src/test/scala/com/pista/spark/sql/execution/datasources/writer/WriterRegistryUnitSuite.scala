package com.pista.spark.sql.execution.datasources.writer

import com.pista.spark.errors.PistaWriterException
import org.scalatest.funsuite.AnyFunSuite

/**
 * WriterRegistry lightweight test
 *
 * Validate only the error messages for unsupported formats to avoid depending on a specific implementation module.
 */
class WriterRegistryUnitSuite extends AnyFunSuite {

  test("Unsupported format should throw an exception") {
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

    val message = exception.getMessage
    assert(message.contains("Writer for format"))
    assert(message.contains("unsupported_format"))
    assert(message.contains("Available writers"))
  }
}
