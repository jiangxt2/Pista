package com.pista.spark.sql.catalyst.functions.scalar

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.datetime.DateTimeKernel
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{ArrayType, DateType, LongType, StringType, TimestampType}

object DateTimeExpressions {
  val tryParseDate: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_try_parse_date", children.length, 2)
    requireFoldable("pista_try_parse_date", "formats", children(1).foldable)
    StaticInvoke(
      DateTimeKernel.getClass,
      DateType,
      "tryParseDate",
      children,
      Seq(StringType, ArrayType(StringType, containsNull = false)),
      propagateNull = true,
      returnNullable = true)
  }

  val tryParseTimestamp: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_try_parse_timestamp", children.length, 3)
    requireFoldable("pista_try_parse_timestamp", "formats", children(1).foldable)
    requireFoldable("pista_try_parse_timestamp", "timezone", children(2).foldable)
    StaticInvoke(
      DateTimeKernel.getClass,
      TimestampType,
      "tryParseTimestamp",
      children,
      Seq(StringType, ArrayType(StringType, containsNull = false), StringType),
      propagateNull = true,
      returnNullable = true)
  }

  private def requireArity(name: String, actual: Int, expected: Int): Unit =
    if (actual != expected)
      throw PistaUDFErrors.udfArgumentLengthMismatchError(name, expected, actual)

  private def requireFoldable(name: String, argument: String, foldable: Boolean): Unit =
    if (!foldable)
      throw PistaUDFErrors.functionInvalidInputError(name, s"$argument must be foldable")
}
