package com.pista.spark.sql.catalyst.functions.scalar

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.json.JsonKernel
import com.pista.spark.sql.catalyst.functions.internal.url.DomainKernel
import com.pista.spark.sql.catalyst.functions.internal.url.UrlKernel
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{BooleanType, StringType}

object JsonUrlExpressions {
  val jsonIsValid: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_json_is_valid", children.length, 1)
    StaticInvoke(
      JsonKernel.getClass,
      BooleanType,
      "isValid",
      children,
      Seq(StringType),
      propagateNull = true,
      returnNullable = false)
  }

  val jsonMergePatch: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_json_merge_patch", children.length, 2)
    StaticInvoke(
      JsonKernel.getClass,
      StringType,
      "mergePatch",
      children,
      Seq(StringType, StringType),
      propagateNull = true,
      returnNullable = false)
  }

  val tryUrlDecode: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_try_url_decode", children.length, 1)
    StaticInvoke(
      UrlKernel.getClass,
      StringType,
      "tryDecode",
      children,
      Seq(StringType),
      propagateNull = true,
      returnNullable = true)
  }

  val registrableDomain: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_registrable_domain", children.length, 1)
    StaticInvoke(
      DomainKernel.getClass,
      StringType,
      "registrableDomain",
      children,
      Seq(StringType),
      propagateNull = true,
      returnNullable = true)
  }

  private def requireArity(name: String, actual: Int, expected: Int): Unit =
    if (actual != expected)
      throw PistaUDFErrors.udfArgumentLengthMismatchError(name, expected, actual)
}
