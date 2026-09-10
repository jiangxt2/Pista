package com.pista.spark.sql.catalyst.functions.scalar

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.network.NetworkKernel
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

object NetworkExpressions {
  val ipToBinary: FunctionRegistry.FunctionBuilder = unary(
    "pista_ip_to_binary", BinaryType, "ipToBinary", StringType, returnNullable = false)

  val tryIpToBinary: FunctionRegistry.FunctionBuilder = unary(
    "pista_try_ip_to_binary", BinaryType, "tryIpToBinary", StringType, returnNullable = true)

  val binaryToIp: FunctionRegistry.FunctionBuilder = unary(
    "pista_binary_to_ip", StringType, "binaryToIp", BinaryType, returnNullable = false)

  val ipv4ToLong: FunctionRegistry.FunctionBuilder = unary(
    "pista_ipv4_to_long", LongType, "ipv4ToLong", StringType, returnNullable = false)

  val longToIpv4: FunctionRegistry.FunctionBuilder = unary(
    "pista_long_to_ipv4", StringType, "longToIpv4", LongType, returnNullable = false)

  val ipFamily: FunctionRegistry.FunctionBuilder = unary(
    "pista_ip_family", IntegerType, "ipFamily", StringType, returnNullable = false)

  val ipIsPrivate: FunctionRegistry.FunctionBuilder = unary(
    "pista_ip_is_private", BooleanType, "ipIsPrivate", StringType, returnNullable = false)

  val ipInCidr: FunctionRegistry.FunctionBuilder = binary(
    "pista_ip_in_cidr", BooleanType, "ipInCidr", Seq(StringType, StringType), returnNullable = false)

  val ipPrefix: FunctionRegistry.FunctionBuilder = binary(
    "pista_ip_prefix", BinaryType, "ipPrefix", Seq(StringType, IntegerType), returnNullable = false)

  private def unary(
      name: String,
      outputType: DataType,
      method: String,
      inputType: DataType,
      returnNullable: Boolean): FunctionRegistry.FunctionBuilder = children => {
    requireArity(name, children, 1)
    invoke(outputType, method, children, Seq(inputType), returnNullable)
  }

  private def binary(
      name: String,
      outputType: DataType,
      method: String,
      inputTypes: Seq[DataType],
      returnNullable: Boolean): FunctionRegistry.FunctionBuilder = children => {
    requireArity(name, children, 2)
    invoke(outputType, method, children, inputTypes, returnNullable)
  }

  private def invoke(
      outputType: DataType,
      method: String,
      children: Seq[Expression],
      inputTypes: Seq[DataType],
      returnNullable: Boolean): Expression =
    StaticInvoke(
      NetworkKernel.getClass,
      outputType,
      method,
      children,
      inputTypes,
      propagateNull = true,
      returnNullable = returnNullable)

  private def requireArity(name: String, children: Seq[Expression], expected: Int): Unit =
    if (children.length != expected)
      throw PistaUDFErrors.udfArgumentLengthMismatchError(name, expected, children.length)
}
