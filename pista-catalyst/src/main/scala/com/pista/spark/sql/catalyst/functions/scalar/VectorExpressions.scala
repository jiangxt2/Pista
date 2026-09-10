package com.pista.spark.sql.catalyst.functions.scalar

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.vector.VectorKernel
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

object VectorExpressions {
  private val VectorType = ArrayType(DoubleType, containsNull = false)

  val innerProduct: FunctionRegistry.FunctionBuilder =
    binary("pista_vector_inner_product", "innerProduct")
  val l2Distance: FunctionRegistry.FunctionBuilder =
    binary("pista_vector_l2_distance", "l2Distance")
  val cosineSimilarity: FunctionRegistry.FunctionBuilder =
    binary("pista_vector_cosine_similarity", "cosineSimilarity")
  val l2Normalize: FunctionRegistry.FunctionBuilder = children => {
    requireArity("pista_vector_l2_normalize", children, 1)
    invoke(VectorType, "l2Normalize", children, Seq(VectorType), returnNullable = false)
  }

  private def binary(name: String, method: String): FunctionRegistry.FunctionBuilder = children => {
    requireArity(name, children, 2)
    invoke(DoubleType, method, children, Seq(VectorType, VectorType), returnNullable = false)
  }

  private def invoke(
      outputType: DataType,
      method: String,
      children: Seq[Expression],
      inputTypes: Seq[DataType],
      returnNullable: Boolean): Expression =
    StaticInvoke(
      VectorKernel.getClass,
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
