package com.pista.spark.sql.catalyst.functions.internal.datetime

import com.pista.spark.errors.PistaUDFErrors
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.Locale

object DateTimeKernel {
  private val MaxValueLength = 4096
  private val MaxFormatCount = 16
  private val MaxFormatLength = 128

  def tryParseDate(value: UTF8String, formats: ArrayData): java.lang.Integer = {
    val parsed = validatedFormats("pista_try_parse_date", formats).iterator
      .flatMap(format => parseDate(value.toString, format))
      .take(1)
      .toSeq
      .headOption
    parsed.flatMap { date =>
      try Some(java.lang.Integer.valueOf(Math.toIntExact(date.toEpochDay)))
      catch { case _: ArithmeticException => None }
    }.orNull
  }

  def tryParseTimestamp(
      value: UTF8String,
      formats: ArrayData,
      timezone: UTF8String): java.lang.Long = {
    validateValue("pista_try_parse_timestamp", value.toString)
    val zone = try ZoneId.of(timezone.toString) catch {
      case _: Exception =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_try_parse_timestamp", s"invalid timezone: ${timezone.toString}")
    }
    val parsed = validatedFormats("pista_try_parse_timestamp", formats).iterator
      .flatMap(format => parseTimestamp(value.toString, format, zone))
      .take(1)
      .toSeq
      .headOption
    parsed.flatMap(toMicros).map(java.lang.Long.valueOf).orNull
  }

  private def parseDate(value: String, format: String): Option[LocalDate] = {
    validateValue("pista_try_parse_date", value)
    try Some(LocalDate.parse(value, formatter(format))) catch { case _: Exception => None }
  }

  private def parseTimestamp(value: String, format: String, zone: ZoneId): Option[Instant] = {
    val dateTime = try LocalDateTime.parse(value, formatter(format)) catch {
      case _: Exception => return None
    }
    val offsets = zone.getRules.getValidOffsets(dateTime)
    if (offsets.size() != 1) None
    else Some(ZonedDateTime.ofStrict(dateTime, offsets.get(0), zone).toInstant)
  }

  private def validatedFormats(functionName: String, formats: ArrayData): Seq[String] = {
    if (formats.numElements() == 0 || formats.numElements() > MaxFormatCount)
      throw PistaUDFErrors.functionInvalidInputError(
        functionName, s"formats must contain between 1 and $MaxFormatCount patterns")
    (0 until formats.numElements()).map { index =>
      if (formats.isNullAt(index))
        throw PistaUDFErrors.functionInvalidInputError(functionName, "formats cannot contain null")
      val format = formats.getUTF8String(index).toString
      if (format.isEmpty || format.length > MaxFormatLength)
        throw PistaUDFErrors.functionInvalidInputError(
          functionName, s"format length must be between 1 and $MaxFormatLength")
      try formatter(format) catch {
        case _: IllegalArgumentException =>
          throw PistaUDFErrors.functionInvalidInputError(functionName, s"invalid format: $format")
      }
      format
    }
  }

  private def validateValue(functionName: String, value: String): Unit =
    if (value.length > MaxValueLength)
      throw PistaUDFErrors.functionResourceLimitError(
        functionName, "input characters", MaxValueLength, value.length)

  private def formatter(pattern: String): DateTimeFormatter =
    DateTimeFormatter.ofPattern(pattern, Locale.ROOT).withResolverStyle(ResolverStyle.STRICT)

  private def toMicros(instant: Instant): Option[Long] =
    try Some(Math.addExact(
      Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L))
    catch { case _: ArithmeticException => None }
}
