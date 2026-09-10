package com.pista.spark.util

import com.pista.spark.errors.PistaConfigException
import org.scalatest.funsuite.AnyFunSuite

/**
 */
class ParamTypeParserSuite extends AnyFunSuite {

  // ==================== Default Type (String) ====================

  test("No type declaration - Return raw string") {
    assert(ParamTypeParser.parseTypedValue("hello", None) === "hello")
  }

  test("No type declaration - Keep numeric string as String") {
    assert(ParamTypeParser.parseTypedValue("12345", None) === "12345")
  }

  // ==================== Int ====================

  test("int Type - Normal Parsing") {
    assert(ParamTypeParser.parseTypedValue("42", Some("int")) === 42)
  }

  test("int type - negative numbers") {
    assert(ParamTypeParser.parseTypedValue("-100", Some("int")) === -100)
  }

  test("int type - illegal value throws exception") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("abc", Some("int"))
    }
  }

  test("int type - overflow throws exception") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("99999999999", Some("int"))
    }
  }

  // ==================== Long ====================

  test("long Type - Normal Parsing") {
    assert(ParamTypeParser.parseTypedValue("12345678901234", Some("long")) === 12345678901234L)
  }

  test("long Type - Invalid Values Throw Exception") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("not_a_number", Some("long"))
    }
  }

  // ==================== Double ====================

  test("double Type - Normal Parsing") {
    assert(ParamTypeParser.parseTypedValue("3.14", Some("double")) === 3.14)
  }

  test("double type - integer value") {
    assert(ParamTypeParser.parseTypedValue("100", Some("double")) === 100.0)
  }

  test("double type - exception thrown for illegal values") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("xyz", Some("double"))
    }
  }

  // ==================== Boolean ====================

  test("boolean Type - true") {
    assert(ParamTypeParser.parseTypedValue("true", Some("boolean")) === true)
  }

  test("boolean Type - false") {
    assert(ParamTypeParser.parseTypedValue("false", Some("boolean")) === false)
  }

  // ==================== Date ====================

  test("date Type - Normal Parsing") {
    val result = ParamTypeParser.parseTypedValue("2024-01-15", Some("date"))
    assert(result === java.sql.Date.valueOf("2024-01-15"))
  }

  test("date type - throws exception for invalid format") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("2024/01/15", Some("date"))
    }
  }

  // ==================== Timestamp ====================

  test("timestamp Type - Normal Parsing") {
    val result = ParamTypeParser.parseTypedValue("2024-01-15 10:30:00", Some("timestamp"))
    assert(result === java.sql.Timestamp.valueOf("2024-01-15 10:30:00"))
  }

  test("timestamp type - throws exception for invalid format") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("not-a-timestamp", Some("timestamp"))
    }
  }

  // ==================== Unknown Type ====================

  test("Unknown Type - Throw Exception") {
    assertThrows[PistaConfigException] {
      ParamTypeParser.parseTypedValue("value", Some("unknown_type"))
    }
  }

  // ==================== Case Insensitivity ====================

  test("Type declaration is case insensitive") {
    assert(ParamTypeParser.parseTypedValue("42", Some("INT")) === 42)
    assert(ParamTypeParser.parseTypedValue("42", Some("Long")) === 42L)
    assert(ParamTypeParser.parseTypedValue("true", Some("BOOLEAN")) === true)
  }
}
