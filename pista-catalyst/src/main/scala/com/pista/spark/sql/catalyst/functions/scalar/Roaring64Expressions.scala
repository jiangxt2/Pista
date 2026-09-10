package com.pista.spark.sql.catalyst.functions.scalar

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.roaring.Roaring64Kernel
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types._

object Roaring64Expressions {
  val fromArray: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_roaring64_from_array", children, 1)
    invoke(
      BinaryType,
      "fromArray",
      children,
      Seq(ArrayType(LongType, containsNull = false)),
      returnNullable = false)
  }

  val union: FunctionRegistry.FunctionBuilder = binary("pista_roaring64_union", "union", BinaryType)
  val intersect: FunctionRegistry.FunctionBuilder =
    binary("pista_roaring64_intersect", "intersect", BinaryType)
  val xor: FunctionRegistry.FunctionBuilder = binary("pista_roaring64_xor", "xor", BinaryType)
  val andNot: FunctionRegistry.FunctionBuilder =
    binary("pista_roaring64_and_not", "andNot", BinaryType)

  val cardinality: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_roaring64_cardinality", children, 1)
    invoke(LongType, "cardinality", children, Seq(BinaryType), returnNullable = false)
  }

  val contains: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_roaring64_contains", children, 2)
    invoke(BooleanType, "contains", children, Seq(BinaryType, LongType), returnNullable = false)
  }

  private def binary(
      name: String,
      method: String,
      outputType: DataType): FunctionRegistry.FunctionBuilder = children => {
    requireArity(name, children, 2)
    invoke(outputType, method, children, Seq(BinaryType, BinaryType), returnNullable = false)
  }

  private def invoke(
      outputType: DataType,
      method: String,
      children: Seq[Expression],
      inputTypes: Seq[DataType],
      returnNullable: Boolean): Expression =
    StaticInvoke(
      Roaring64Kernel.getClass,
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
