package com.pista.spark.errors

import org.apache.spark.SparkThrowable

import scala.jdk.CollectionConverters._

/**
 * Pista configuration-related exception
 *
 */
class PistaConfigException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends IllegalArgumentException(
    PistaErrorMessageLoader.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}

/**
 * Pista SQL file-related exceptions
 */
class PistaSQLFileException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends java.io.FileNotFoundException(
    PistaErrorMessageLoader.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}

/** Pista job-execution exception with structured error metadata. */
class PistaExecutionException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends RuntimeException(
    PistaErrorMessageLoader.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}

/**
 * Pista Writer Exception
 */
class PistaWriterException(
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

/**
 * Pista Reader related exceptions
 */
class PistaReaderException(
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

/**
 * Pista Processor related exceptions
 */
class PistaProcessorException(
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

/**
 * Pista Checkpoint Exception Related
 */
class PistaCheckpointException(
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

/**
 * Pista ClickHouse batch write related exception
 */
class PistaClickHouseException(
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

/**
 * Pista Doris write batch exception related
 */
class PistaDorisException(
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
