package com.pista.spark.sql.execution

import org.apache.spark.SparkException
import org.scalatest.funsuite.AnyFunSuite

/**
 * ExecutionReport Unit Test
 *
 */
class ExecutionReportSuite extends AnyFunSuite {

  test("Newly created report should be empty") {
    val report = new ExecutionReport()

    assert(report.totalCount == 0)
    assert(report.successCount == 0)
    assert(report.failureCount == 0)
    assert(report.skippedCount == 0)
    assert(!report.hasFailures)
  }

  test("addSuccess records a successful statement") {
    val report = new ExecutionReport()
    report.addSuccess(1, "SELECT 1", 100)

    assert(report.totalCount == 1)
    assert(report.successCount == 1)
    assert(report.failureCount == 0)
    assert(!report.hasFailures)
  }

  test("addFailure should correctly record failure") {
    val report = new ExecutionReport()
    val error = new SparkException(
      errorClass = "PISTA_TEST_ERROR",
      messageParameters = Map("msg" -> "test"),
      cause = null
    )
    report.addFailure(1, "SELECT * FROM invalid", error, 50)

    assert(report.totalCount == 1)
    assert(report.successCount == 0)
    assert(report.failureCount == 1)
    assert(report.hasFailures)
  }

  test("addSkipped records a skipped result") {
    val report = new ExecutionReport()
    report.addSkipped(1, "SELECT 1")

    assert(report.totalCount == 1)
    assert(report.skippedCount == 1)
  }

  test("replaceWithFailure converts an existing success without changing total count") {
    val report = new ExecutionReport()
    report.addSuccess(1, "SELECT 1", 10)

    report.replaceWithFailure(1, "SELECT 1", new RuntimeException("private-value"), 20)

    assert(report.totalCount === 1)
    assert(report.successCount === 0)
    assert(report.failureCount === 1)
    assert(!report.formatReport.contains("private-value"))
  }

  test("correctly aggregate mixed results") {
    val report = new ExecutionReport()

    report.addSuccess(1, "SELECT 1", 100)
    report.addSuccess(2, "SELECT 2", 150)
    report.addFailure(3, "SELECT 3", new RuntimeException("error"), 50)
    report.addSkipped(4, "SELECT 4")
    report.addSuccess(5, "SELECT 5", 200)

    assert(report.totalCount == 5)
    assert(report.successCount == 3)
    assert(report.failureCount == 1)
    assert(report.skippedCount == 1)
    assert(report.hasFailures)
  }

  test("failures returns only failed results") {
    val report = new ExecutionReport()

    report.addSuccess(1, "SELECT 1", 100)
    report.addFailure(2, "SELECT 2", new RuntimeException("error1"), 50)
    report.addFailure(3, "SELECT 3", new RuntimeException("error2"), 60)
    report.addSuccess(4, "SELECT 4", 120)

    val failures = report.failures

    assert(failures.length == 2)
    assert(failures.forall(_.status == ExecutionStatus.Failed))
    assert(failures.map(_.sqlIndex).toSet == Set(2, 3))
  }

  test("formatReport should generate the correct report format") {
    val report = new ExecutionReport()

    report.addSuccess(1, "SELECT 1", 100)
    report.addFailure(2, "SELECT * FROM invalid_table", new RuntimeException("Table not found"), 50)
    report.addSuccess(3, "SELECT 3", 150)

    val formatted = report.formatReport

    assert(formatted.contains("Pista Job Execution Report"))
    assert(formatted.contains("Total SQL statements: 3"))
    assert(formatted.contains("Succeeded: 2"))
    assert(formatted.contains("Failed: 1"))
    assert(formatted.contains("Failure details:"))
    assert(formatted.contains("SQL statement 2 failed:"))
    assert(formatted.contains("SQL fingerprint:"))
    assert(formatted.contains("Error type: RuntimeException"))
    assert(!formatted.contains("invalid_table"))
    assert(!formatted.contains("Table not found"))
  }

  test("formatReport does not display failure details when not failed") {
    val report = new ExecutionReport()

    report.addSuccess(1, "SELECT 1", 100)
    report.addSuccess(2, "SELECT 2", 150)

    val formatted = report.formatReport

    assert(formatted.contains("Succeeded: 2"))
    assert(formatted.contains("Failed: 0"))
    assert(!formatted.contains("Failure details:"))
  }

  test("correctly extract error codes from SparkThrowable") {
    val report = new ExecutionReport()
    val error = new SparkException(
      errorClass = "PISTA_TEST_ERROR",
      messageParameters = Map("msg" -> "test message"),
      cause = null
    )

    report.addFailure(1, "SELECT 1", error, 100)

    val failures = report.failures
    assert(failures.head.errorClass.contains("PISTA_TEST_ERROR"))
  }

  test("non-Spark throwable exceptions should be handled correctly SparkThrowable exception occurs") {
    val report = new ExecutionReport()
    val error = new RuntimeException("Generic error")

    report.addFailure(1, "SELECT 1", error, 100)

    val failures = report.failures
    assert(failures.head.errorClass.isEmpty)
    assert(failures.head.errorType.contains("RuntimeException"))
    assert(!report.formatReport.contains("Generic error"))
  }

  test("should record execution time") {
    val report = new ExecutionReport()

    report.addSuccess(1, "SELECT 1", 100)
    report.addFailure(2, "SELECT 2", new RuntimeException("error"), 50)

    val results = report.failures ++ Seq(
      ExecutionResult(1, "e004ebd5b553", ExecutionStatus.Success, durationMs = 100)
    )

    assert(results.exists(_.durationMs == 100))
    assert(results.exists(_.durationMs == 50))
  }
}
