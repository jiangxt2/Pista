package com.pista.spark.sql.errors

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import org.apache.spark.sql.SparkSession

/**
 * errorHandlingStrategy
 *
 */
sealed trait ErrorPolicy {
  def shouldFailFast: Boolean
}

object ErrorPolicy {
  case object FailFast extends ErrorPolicy {
    override def shouldFailFast: Boolean = true
  }

  case object ContinueOnError extends ErrorPolicy {
    override def shouldFailFast: Boolean = false
  }

  def fromString(value: String, configKey: String = "spark.pista.errorPolicy"): ErrorPolicy = value.toLowerCase match {
    case "fail-fast" => FailFast
    case "continue-on-error" => ContinueOnError
    case _ =>
      throw PistaErrors.invalidConfigValueError(
        configKey = configKey,
        value = value,
        expected = "fail-fast or continue-on-error")
  }
}

/**
 **error parser**
 *
 * Support global strategy + module override
 */
class ErrorPolicyResolver(spark: SparkSession) {
  private val conf = ConfigReader(spark)

  // Global strategy
  private lazy val globalPolicy: ErrorPolicy = {
    val value = conf.get(SubmitterConf.ERROR_POLICY)
    ErrorPolicy.fromString(value, SubmitterConf.ERROR_POLICY.key)
  }

  // Get the error strategy for SQL execution
  def sqlErrorPolicy: ErrorPolicy = {
    conf.get(SubmitterConf.SQL_ERROR_POLICY)
      .map(v => ErrorPolicy.fromString(v, SubmitterConf.SQL_ERROR_POLICY.key))
      .getOrElse(globalPolicy)
  }

  // Obtain the error strategy of the Processor
  def processorErrorPolicy: ErrorPolicy = {
    conf.get(SubmitterConf.PROCESSOR_ERROR_POLICY)
      .map(v => ErrorPolicy.fromString(v, SubmitterConf.PROCESSOR_ERROR_POLICY.key))
      .getOrElse(globalPolicy)
  }

  // Obtain the error strategy for Output
  def outputErrorPolicy: ErrorPolicy = {
    conf.get(SubmitterConf.OUTPUT_ERROR_POLICY)
      .map(v => ErrorPolicy.fromString(v, SubmitterConf.OUTPUT_ERROR_POLICY.key))
      .getOrElse(globalPolicy)
  }

  // Resolve the checkpoint error policy.
  def checkpointErrorPolicy: ErrorPolicy = {
    conf.get(SubmitterConf.CHECKPOINT_ERROR_POLICY)
      .map(v => ErrorPolicy.fromString(v, SubmitterConf.CHECKPOINT_ERROR_POLICY.key))
      .getOrElse(globalPolicy)
  }
}
