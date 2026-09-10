package com.pista.spark.errors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable
import scala.io.Source

/**
 * Pista error message loader
 *
 * from error_classes_json error-classes.json Load the error code definitions and generate error messages
 * Reference ErrorClassesJsonReader implementation in Spark
 *
 */
object PistaErrorMessageLoader {

  private val errorClassToInfoMap: Map[String, ErrorInfo] = loadErrorClasses()

  /**
   * Load error code definitions
   */
  private def loadErrorClasses(): Map[String, ErrorInfo] = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val url = classLoader.getResource("error/error-classes.json")

    if (url == null) {
      throw new RuntimeException("Cannot find error/error-classes.json in classpath")
    }

    val source = Source.fromURL(url, "UTF-8")
    try {
      val json = source.mkString
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      // parsed into a Scala map Scala Map
      val errorMap = mapper.readValue(json, classOf[Map[String, Map[String, Any]]])

      val result = mutable.Map[String, ErrorInfo]()

      errorMap.foreach { case (errorClass, info) =>
        val messageTemplate = info.get("message") match {
          case Some(list: Seq[_]) =>
            list.map(_.toString).mkString("\n")
          case Some(str: String) => str
          case _ => ""
        }

        val sqlState = info.get("sqlState") match {
          case Some(state: String) => state
          case _ => ""
        }

        result(errorClass) = ErrorInfo(messageTemplate, sqlState)
      }

      result.toMap
    } finally {
      source.close()
    }
  }

  /**
   * Retrieve error message
   *
   * @param errorClass Error Code
   * @param messageParameters Message Parameters
   * @return Formatted error message
   */
  def getMessage(errorClass: String, messageParameters: Map[String, String]): String = {
    errorClassToInfoMap.get(errorClass) match {
      case Some(errorInfo) =>
        var message = errorInfo.messageTemplate

        // Replace placeholder parameters <paramName>
        messageParameters.foreach { case (key, value) =>
          message = message.replace(s"<$key>", value)
        }

        s"[$errorClass] $message"

      case None =>
        // If no error code definition is found, return a simple message.
        s"[$errorClass] Error with parameters: ${messageParameters.mkString(", ")}"
    }
  }

  /**
   * Get SQL State
   */
  def getSqlState(errorClass: String): Option[String] = {
    errorClassToInfoMap.get(errorClass).flatMap { info =>
      if (info.sqlState.nonEmpty) Some(info.sqlState) else None
    }
  }

  /**
   * error message
   */
  private case class ErrorInfo(messageTemplate: String, sqlState: String)
}
