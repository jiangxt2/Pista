package com.pista.spark.sql.test.util

import java.util.{Locale, UUID}
import java.util.concurrent.atomic.AtomicLong

/**
 * Generates run-unique identifiers for database objects and S3 buckets.
 *
 * The file name intentionally does not start with `Test`: the user-level
 * gitignore contains `Test*.scala`, while existing callers use the
 * `TestDataGenerator` object name.
 */
object TestDataGenerator {
  private val IdentifierMaxLength = 63
  private val BucketMaxLength     = 63
  private val Counter             = new AtomicLong()

  private val InvalidIdentifier = "[^a-z0-9_]+".r
  private val InvalidBucket     = "[^a-z0-9-]+".r

  def uniqueName(prefix: String): String = {
    val raw = requirePrefix(prefix)
    val normalized = InvalidIdentifier
      .replaceAllIn(raw.toLowerCase(Locale.ROOT), "_")
      .replaceAll("_+", "_")
      .stripPrefix("_")
      .stripSuffix("_")
    val base = Option(normalized).filter(_.nonEmpty).getOrElse("pista_it")
    val safeBase = if (base.head.isDigit) s"it_$base" else base
    appendSuffix(safeBase, "_", IdentifierMaxLength)
  }

  def uniqueBucketName(prefix: String): String = {
    val raw = requirePrefix(prefix)
    val normalized = InvalidBucket
      .replaceAllIn(raw.toLowerCase(Locale.ROOT), "-")
      .replaceAll("-+", "-")
      .stripPrefix("-")
      .stripSuffix("-")
    val base = Option(normalized).filter(_.nonEmpty).getOrElse("pista-it")
    appendSuffix(base, "-", BucketMaxLength)
  }

  private def requirePrefix(prefix: String): String =
    Option(prefix).map(_.trim).filter(_.nonEmpty).getOrElse(
      throw new IllegalArgumentException("prefix must not be blank"))

  private def appendSuffix(base: String, separator: String, maxLength: Int): String = {
    val sequence = java.lang.Long.toUnsignedString(Counter.incrementAndGet(), 36)
    val random   = UUID.randomUUID().toString.replace("-", "").take(10)
    val suffix   = s"$sequence$random"
    val maxBaseLength = maxLength - separator.length - suffix.length
    val shortened = base.take(maxBaseLength).stripSuffix(separator)
    s"$shortened$separator$suffix"
  }
}
