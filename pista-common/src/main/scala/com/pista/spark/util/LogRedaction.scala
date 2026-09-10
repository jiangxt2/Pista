package com.pista.spark.util

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Helpers for logging stable diagnostics without business data or credentials. */
object LogRedaction {

  def fingerprint(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(Option(value).getOrElse("").getBytes(StandardCharsets.UTF_8))
    digest.take(6).map(byte => f"${byte & 0xff}%02x").mkString
  }

  def exceptionName(error: Throwable): String =
    Option(error).map(_.getClass.getSimpleName).filter(_.nonEmpty).getOrElse("UnknownError")

  def sanitizedThrowable(error: Throwable): RuntimeException = {
    val sanitized = new RuntimeException(s"${exceptionName(error)} (details redacted)")
    if (error != null) sanitized.setStackTrace(error.getStackTrace)
    sanitized
  }

  def optionKeys(options: collection.Map[String, _]): String =
    Option(options).map(_.keys.toSeq.sorted.mkString(",")).filter(_.nonEmpty).getOrElse("<none>")
}
