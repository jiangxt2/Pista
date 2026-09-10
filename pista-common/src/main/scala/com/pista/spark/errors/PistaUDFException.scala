package com.pista.spark.errors

import org.apache.spark.SparkThrowable

import scala.jdk.CollectionConverters._

/**
 * Pista UDF-related exceptions
 *
 */
class PistaUDFException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable)
  extends RuntimeException(
    PistaErrorMessageLoader.getMessage(errorClass, messageParameters),
    cause)
  with SparkThrowable {

  def this(errorClass: String, messageParameters: Map[String, String]) =
    this(errorClass, messageParameters, null)

  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}
