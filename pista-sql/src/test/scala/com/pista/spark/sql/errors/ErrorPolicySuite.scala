package com.pista.spark.sql.errors

import com.pista.spark.errors.PistaConfigException
import com.pista.spark.sql.conf.SubmitterConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * ErrorPolicy Unit Test
 *
 */
class ErrorPolicySuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("ErrorPolicySuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("fromString should correctly parse fail-fast") {
    val policy = ErrorPolicy.fromString("fail-fast")
    assert(policy == ErrorPolicy.FailFast)
    assert(policy.shouldFailFast)
  }

  test("fromString should correctly parse continue-on-error") {
    val policy = ErrorPolicy.fromString("continue-on-error")
    assert(policy == ErrorPolicy.ContinueOnError)
    assert(!policy.shouldFailFast)
  }

  test("fromString is case insensitive") {
    assert(ErrorPolicy.fromString("FAIL-FAST") == ErrorPolicy.FailFast)
    assert(ErrorPolicy.fromString("Continue-On-Error") == ErrorPolicy.ContinueOnError)
  }

  test("fromString an exception should be thrown for invalid values") {
    val error = intercept[PistaConfigException] {
      ErrorPolicy.fromString("invalid")
    }
    assert(error.getErrorClass == "PISTA_CONFIG_INVALID_VALUE")
    assert(error.getMessage.contains("fail-fast or continue-on-error"))
  }

  test("ErrorPolicyResolver should use the default global policy") {
    val resolver = new ErrorPolicyResolver(spark)
    val policy = resolver.sqlErrorPolicy

    // Default should be fail-fast.
    assert(policy.shouldFailFast)
  }

  test("ErrorPolicyResolver should support global configuration") {
    spark.conf.set(SubmitterConf.ERROR_POLICY.key, "continue-on-error")

    val resolver = new ErrorPolicyResolver(spark)
    val policy = resolver.sqlErrorPolicy

    assert(!policy.shouldFailFast)

    // Clean
    spark.conf.unset(SubmitterConf.ERROR_POLICY.key)
  }

  test("ErrorPolicyResolver should support module-level overrides") {
    spark.conf.set(SubmitterConf.ERROR_POLICY.key, "fail-fast")
    spark.conf.set(SubmitterConf.SQL_ERROR_POLICY.key, "continue-on-error")

    val resolver = new ErrorPolicyResolver(spark)

    // SQL Policy should override global policies
    assert(!resolver.sqlErrorPolicy.shouldFailFast)

    // Other modules should use the global strategy
    assert(resolver.processorErrorPolicy.shouldFailFast)
    assert(resolver.outputErrorPolicy.shouldFailFast)

    // Clean
    spark.conf.unset(SubmitterConf.ERROR_POLICY.key)
    spark.conf.unset(SubmitterConf.SQL_ERROR_POLICY.key)
  }

  test("ErrorPolicyResolver It should support policy configurations for all modules.") {
    spark.conf.set(SubmitterConf.ERROR_POLICY.key, "fail-fast")
    spark.conf.set(SubmitterConf.SQL_ERROR_POLICY.key, "continue-on-error")
    spark.conf.set(SubmitterConf.PROCESSOR_ERROR_POLICY.key, "continue-on-error")
    spark.conf.set(SubmitterConf.OUTPUT_ERROR_POLICY.key, "fail-fast")
    spark.conf.set(SubmitterConf.CHECKPOINT_ERROR_POLICY.key, "continue-on-error")

    val resolver = new ErrorPolicyResolver(spark)

    assert(!resolver.sqlErrorPolicy.shouldFailFast)
    assert(!resolver.processorErrorPolicy.shouldFailFast)
    assert(resolver.outputErrorPolicy.shouldFailFast)
    assert(!resolver.checkpointErrorPolicy.shouldFailFast)

    // Clean
    spark.conf.unset(SubmitterConf.ERROR_POLICY.key)
    spark.conf.unset(SubmitterConf.SQL_ERROR_POLICY.key)
    spark.conf.unset(SubmitterConf.PROCESSOR_ERROR_POLICY.key)
    spark.conf.unset(SubmitterConf.OUTPUT_ERROR_POLICY.key)
    spark.conf.unset(SubmitterConf.CHECKPOINT_ERROR_POLICY.key)
  }
}
