package com.pista.spark.util

import org.scalatest.funsuite.AnyFunSuite

/**
 */
class SQLStatementSplitterSuite extends AnyFunSuite {

  test("empty SQL returns no statements") {
    val result = SQLStatementSplitter.split("")
    assert(result.isEmpty)
  }

  test("null input") {
    val result = SQLStatementSplitter.split(null)
    assert(result.isEmpty)
  }

  test("Simple single-statement SQL") {
    val sql = "SELECT * FROM table"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT * FROM table")
  }

  test("Simple single-statement SQL - With semicolon") {
    val sql = "SELECT * FROM table;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT * FROM table")
  }

  test("Simple Multi-statement SQL") {
    val sql = "SELECT 1; SELECT 2; SELECT 3"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 3)
    assert(result(0) == "SELECT 1")
    assert(result(1) == " SELECT 2")
    assert(result(2) == " SELECT 3")
  }

  test("String characters with semicolon - single quote") {
    val sql = "SELECT 'value;with;semicolon' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'value;with;semicolon' as col")
  }

  test("A semicolon in the string - quoted strings") {
    val sql = "SELECT \"value;with;semicolon\" as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT \"value;with;semicolon\" as col")
  }

  test("semicolon in strings - quoted strings") {
    val sql = "SELECT 'single;quote', \"double;quote\" as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'single;quote', \"double;quote\" as col")
  }

  test("row comment semicolon") {
    val sql =
      """-- comment; with semicolon
        |SELECT * FROM table;""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0).contains("SELECT * FROM table"))
  }

  test("A semicolon in row comments - lines") {
    val sql =
      """-- first comment; with semicolon
        |SELECT 1;
        |-- second comment; also with semicolon
        |SELECT 2;""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0).contains("SELECT 1"))
    assert(result(1).contains("SELECT 2"))
  }

  test("Semicolons in block comments are ignored.") {
    val sql = "/* comment; with semicolon */ SELECT * FROM table;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "/* comment; with semicolon */ SELECT * FROM table")
  }

  test("Nested block comments") {
    val sql = "SELECT /* outer /* nested */ comment */ * FROM table;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT /* outer /* nested */ comment */ * FROM table")
  }

  test("Nested Comment Blocks - Multi-Level Nesting") {
    val sql = "SELECT /* level1 /* level2 /* level3 */ */ */ * FROM table;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0).contains("SELECT"))
    assert(result(0).contains("FROM table"))
  }

  test("Escaped Characters - Single Quote") {
    val sql = "SELECT 'it\\'s a test;' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'it\\'s a test;' as col")
  }

  test("Escaped Characters - Backslash") {
    val sql = "SELECT 'path\\\\to\\\\file;' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'path\\\\to\\\\file;' as col")
  }

  test("escape characters - within") {
    val sql = "SELECT \"it's ok;\" as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT \"it's ok;\" as col")
  }

  test("Escaped Characters - within single quotes") {
    val sql = "SELECT 'say \"hello\";' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'say \"hello\";' as col")
  }

  test("Empty Statements and Whitespace Handling") {
    val sql = "SELECT 1; ; ; SELECT 2;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0) == "SELECT 1")
    assert(result(1).trim == "SELECT 2")
  }

  test("No statements return with pure comments. - row comment") {
    val sql = "-- only comment;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.isEmpty)
  }

  test("No statements return due to pure comments - block comment") {
    val sql = "/* only comment */;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.isEmpty)
  }

  test("end-of-line complete comment") {
    val sql = "SELECT 1; /* trailing comment */"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 1")
  }

  test("Multiple-row SQL") {
    val sql =
      """SELECT *
        |FROM table
        |WHERE col = 'value;';""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0).contains("SELECT *"))
    assert(result(0).contains("FROM table"))
    assert(result(0).contains("WHERE col = 'value;'"))
  }

  test("WITH Clause") {
    val sql = "WITH cte AS (SELECT 'a;b' as col) SELECT * FROM cte;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "WITH cte AS (SELECT 'a;b' as col) SELECT * FROM cte")
  }

  test("CASE WHEN Statement") {
    val sql = "SELECT CASE WHEN col = 'a;b' THEN 1 ELSE 0 END as result;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT CASE WHEN col = 'a;b' THEN 1 ELSE 0 END as result")
  }

  test("Mixed Scenario - comment + string + multi-statements") {
    val sql =
      """-- Data cleaning
        |CREATE TABLE tmp_users AS
        |SELECT * FROM raw_users WHERE status = 'active;pending';
        |
        |-- Aggregates data
        |INSERT INTO agg_table
        |SELECT date, COUNT(*) as cnt
        |FROM tmp_users
        |GROUP BY date;""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0).contains("CREATE TABLE"))
    assert(result(0).contains("active;pending"))
    assert(result(1).contains("INSERT INTO"))
  }

  test("Comments inside quotes do not take effect") {
    val sql = "SELECT 'this is -- not a comment' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'this is -- not a comment' as col")
  }

  test("Column comments within quotes are ineffective.") {
    val sql = "SELECT 'this is /* not a comment */' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'this is /* not a comment */' as col")
  }

  test("Quotation marks do not work in row comments.") {
    val sql =
      """-- this is a comment with 'quotes'
        |SELECT 1;""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0).contains("SELECT 1"))
  }

  test("Quotation marks do not work within block comments.") {
    val sql = "/* comment with 'quotes' */ SELECT 1;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "/* comment with 'quotes' */ SELECT 1")
  }

  test("complex real-world scenario") {
    val sql =
      """-- create a temporary table
        |CREATE TEMPORARY VIEW temp_view AS
        |SELECT
        |  id,
        |  name,
        |  CASE
        |    WHEN status = 'active;pending' THEN 1
        |    WHEN status = 'inactive' THEN 0
        |  END as status_code
        |FROM users
        |WHERE created_at > '2024-01-01';
        |
        |-- Query Results
        |SELECT /* statistics */ COUNT(*) as total
        |FROM temp_view
        |WHERE status_code = 1;""".stripMargin
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0).contains("CREATE TEMPORARY VIEW"))
    assert(result(0).contains("active;pending"))
    assert(result(1).contains("SELECT /* statistics */ COUNT(*)"))
  }

  test("Incomplete block comments should return") {
    val sql = "SELECT 1; /* unclosed comment"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0) == "SELECT 1")
    assert(result(1) == " /* unclosed comment")
  }

  test("Escaped newline characters within a string") {
    val sql = "SELECT 'line1\\\nline2' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0).contains("line1"))
    assert(result(0).contains("line2"))
  }

  test("block comment end boundary") {
    val sql = "SELECT /* a */ * FROM t;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT /* a */ * FROM t")
  }

  test("consecutive block comments") {
    val sql = "SELECT /* comment1 */ /* comment2 */ * FROM t;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT /* comment1 */ /* comment2 */ * FROM t")
  }

  test("row comment followed immediately by a statement") {
    val sql = "SELECT 1; -- comment\nSELECT 2;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 2)
    assert(result(0) == "SELECT 1")
    assert(result(1).contains("SELECT 2"))
  }

  test("Only whitespace and semicolons") {
    val sql = "   ;   ;   "
    val result = SQLStatementSplitter.split(sql)
    assert(result.isEmpty)
  }

  test("escaped semi-colon") {
    val sql = "SELECT 'test\\;' as col;"
    val result = SQLStatementSplitter.split(sql)
    assert(result.length == 1)
    assert(result(0) == "SELECT 'test\\;' as col")
  }
}
