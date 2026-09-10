package com.pista.spark.util

import org.scalatest.funsuite.AnyFunSuite

class LogRedactionSuite extends AnyFunSuite {

  test("fingerprint is stable and does not contain the source value") {
    val value = "SELECT * FROM private_table WHERE marker = 'private-value'"
    val fingerprint = LogRedaction.fingerprint(value)

    assert(fingerprint === LogRedaction.fingerprint(value))
    assert(fingerprint.length === 12)
    assert(!fingerprint.contains("private_table"))
  }

  test("optionKeys reports names without values") {
    val summary = LogRedaction.optionKeys(Map("password" -> "test-password", "user" -> "alice"))

    assert(summary === "password,user")
    assert(!summary.contains("test-password"))
    assert(!summary.contains("alice"))
  }

  test("exceptionName omits the exception message") {
    val summary = LogRedaction.exceptionName(new IllegalStateException("private-value"))

    assert(summary === "IllegalStateException")
    assert(!summary.contains("private-value"))
  }

  test("sanitizedThrowable retains the type and stack without the message") {
    val original = new IllegalStateException("private-value")
    val sanitized = LogRedaction.sanitizedThrowable(original)

    assert(sanitized.getMessage.contains("IllegalStateException"))
    assert(!sanitized.getMessage.contains("private-value"))
    assert(sanitized.getStackTrace.sameElements(original.getStackTrace))
  }
}
