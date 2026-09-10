package com.pista.spark.sql.execution.datasources.reader

import org.scalatest.funsuite.AnyFunSuite

/**
 * Reader SPI discovers test
 *
 * Validate ServiceLoader auto-discovery mechanism and query interface of ReaderRegistry.
 * No actual database connection, only testing registry behavior.
 *
 */
class ReaderRegistrySuite extends AnyFunSuite {

  test("availableFormats returns registered formats") {
    val formats = ReaderRegistry.availableFormats
    // ServiceLoader discovered Reader depending classpath on the META-INF/services configuration
    // And do not throw an exception.
    assert(formats != null)
  }

  test("isSupported true for registered formats true") {
    val formats = ReaderRegistry.availableFormats
    formats.foreach { format =>
      assert(ReaderRegistry.isSupported(format), s"format $format should be supported")
    }
  }

  test("isSupported for unsupported formats returns false") {
    assert(!ReaderRegistry.isSupported("nonexistent_format_xyz"))
  }

  test("isSupported should be case insensitive") {
    val formats = ReaderRegistry.availableFormats
    if (formats.nonEmpty) {
      val format = formats.head
      assert(ReaderRegistry.isSupported(format.toUpperCase) == ReaderRegistry.isSupported(format.toLowerCase))
    }
  }

  test("getReader for registered formats should return Some") {
    val formats = ReaderRegistry.availableFormats
    formats.foreach { format =>
      val reader = ReaderRegistry.getReader(format)
      assert(reader.isDefined, s"getReader($format) should return Some")
      assert(reader.get.name == format)
    }
  }

  test("getReader for an unregistered format should return None") {
    assert(ReaderRegistry.getReader("nonexistent_format_xyz").isEmpty)
  }

  test("read for unregistered formats should throw an exception") {
    val config = InputConfig(
      enabled = true,
      path = Some("/tmp/test"),
      format = "nonexistent_format_xyz"
    )

    assertThrows[Exception] {
      ReaderRegistry.read(null, config)
    }
  }

  test("All registered Readers should have a non-empty name") {
    val formats = ReaderRegistry.availableFormats
    formats.foreach { format =>
      val reader = ReaderRegistry.getReader(format).get
      assert(reader.name.nonEmpty, s"Reader name cannot be empty")
    }
  }
}
