package com.pista.spark.util

import com.pista.spark.errors.PistaSQLFileException
import org.scalatest.funsuite.AnyFunSuite

import java.util

class SQLTemplateEngineSuite extends AnyFunSuite {
  test("plain SQL remains unchanged without template parameters") {
    val sql = "SELECT 'literal;value' AS value;"
    assert(SQLTemplateEngine.processSQLTemplate(sql, new util.HashMap[String, String]()) == sql)
  }

  test("template substitution preserves parameter values without reinterpreting them") {
    val params = new util.HashMap[String, String]()
    params.put("table", "pista_input")
    params.put("value", "literal${other};value")

    assert(SQLTemplateEngine.processSQLTemplate(
      "SELECT '${value}' FROM ${table}", params) ==
      "SELECT 'literal${other};value' FROM pista_input")
  }

  test("conditions loops and defaults preserve existing SQL template behavior") {
    val params = new util.HashMap[String, String]()
    params.put("enabled", "yes")
    val sql = "<#if enabled == 'yes'>SELECT <#list 1..3 as i>${i}<#sep>, </#list>" +
      " FROM ${table!'pista_input'}</#if>"

    assert(SQLTemplateEngine.processSQLTemplate(sql, params) == "SELECT 1, 2, 3 FROM pista_input")
    params.put("enabled", "no")
    assert(SQLTemplateEngine.processSQLTemplate(sql, params).isEmpty)
  }

  test("missing required parameters and malformed templates remain SQL parse failures") {
    Seq("SELECT ${missing}", "<#if true>SELECT 1").foreach { sql =>
      intercept[PistaSQLFileException] {
        SQLTemplateEngine.processSQLTemplate(sql, new util.HashMap[String, String]())
      }
    }
  }
}
