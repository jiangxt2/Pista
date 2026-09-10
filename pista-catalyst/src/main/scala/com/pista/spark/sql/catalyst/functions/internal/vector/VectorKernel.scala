package com.pista.spark.sql.catalyst.functions.internal.vector

import com.pista.spark.errors.PistaUDFErrors
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}

object VectorKernel {
  val MaxDimension = 65536

  def innerProduct(left: ArrayData, right: ArrayData): java.lang.Double = {
    val (leftValues, rightValues) = pair("pista_vector_inner_product", left, right)
    java.lang.Double.valueOf(compensatedDot(
      "pista_vector_inner_product", leftValues, rightValues))
  }

  def l2Distance(left: ArrayData, right: ArrayData): java.lang.Double = {
    val (leftValues, rightValues) = pair("pista_vector_l2_distance", left, right)
    var scale = 0.0
    var sumSquares = 1.0
    var index = 0
    while (index < leftValues.length) {
      val difference = Math.abs(leftValues(index) - rightValues(index))
      requireFinite("pista_vector_l2_distance", difference)
      if (difference != 0.0) {
        if (scale < difference) {
          val ratio = scale / difference
          sumSquares = 1.0 + sumSquares * ratio * ratio
          scale = difference
        } else {
          val ratio = difference / scale
          sumSquares += ratio * ratio
        }
      }
      index += 1
    }
    java.lang.Double.valueOf(if (scale == 0.0) 0.0 else scale * Math.sqrt(sumSquares))
  }

  def cosineSimilarity(left: ArrayData, right: ArrayData): java.lang.Double = {
    val (leftValues, rightValues) = pair("pista_vector_cosine_similarity", left, right)
    val leftNorm = norm("pista_vector_cosine_similarity", leftValues)
    val rightNorm = norm("pista_vector_cosine_similarity", rightValues)
    if (leftNorm == 0.0 || rightNorm == 0.0)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_vector_cosine_similarity", "zero vectors do not have a cosine similarity")
    val dot = compensatedDot("pista_vector_cosine_similarity", leftValues, rightValues)
    val result = dot / leftNorm / rightNorm
    requireFinite("pista_vector_cosine_similarity", result)
    java.lang.Double.valueOf(Math.max(-1.0, Math.min(1.0, result)))
  }

  def l2Normalize(value: ArrayData): ArrayData = {
    val values = validated("pista_vector_l2_normalize", value)
    val length = norm("pista_vector_l2_normalize", values)
    if (length == 0.0)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_vector_l2_normalize", "zero vectors cannot be normalized")
    new GenericArrayData(values.map(_ / length))
  }

  def validated(functionName: String, value: ArrayData): Array[Double] = {
    val dimension = value.numElements()
    if (dimension == 0 || dimension > MaxDimension)
      throw PistaUDFErrors.functionInvalidInputError(
        functionName, s"dimension must be in [1, $MaxDimension], got $dimension")
    val result = new Array[Double](dimension)
    var index = 0
    while (index < dimension) {
      if (value.isNullAt(index))
        throw PistaUDFErrors.functionInvalidInputError(functionName, "vectors cannot contain null")
      result(index) = value.getDouble(index)
      requireFinite(functionName, result(index))
      index += 1
    }
    result
  }

  def requireFinite(functionName: String, value: Double): Unit =
    if (!java.lang.Double.isFinite(value))
      throw PistaUDFErrors.functionInvalidInputError(functionName, "vectors and results must be finite")

  private def pair(
      functionName: String,
      left: ArrayData,
      right: ArrayData): (Array[Double], Array[Double]) = {
    val leftValues = validated(functionName, left)
    val rightValues = validated(functionName, right)
    if (leftValues.length != rightValues.length)
      throw PistaUDFErrors.functionInvalidInputError(
        functionName, s"dimensions differ: ${leftValues.length} and ${rightValues.length}")
    (leftValues, rightValues)
  }

  private def norm(functionName: String, values: Array[Double]): Double = {
    var scale = 0.0
    var sumSquares = 1.0
    var index = 0
    while (index < values.length) {
      val value = values(index)
      val absolute = Math.abs(value)
      if (absolute != 0.0) {
        if (scale < absolute) {
          val ratio = scale / absolute
          sumSquares = 1.0 + sumSquares * ratio * ratio
          scale = absolute
        } else {
          val ratio = absolute / scale
          sumSquares += ratio * ratio
        }
      }
      index += 1
    }
    val result = if (scale == 0.0) 0.0 else scale * Math.sqrt(sumSquares)
    requireFinite(functionName, result)
    result
  }

  private def compensatedDot(
      functionName: String,
      left: Array[Double],
      right: Array[Double]): Double = {
    var sum = 0.0
    var compensation = 0.0
    var index = 0
    while (index < left.length) {
      val value = left(index) * right(index)
      requireFinite(functionName, value)
      val adjusted = value - compensation
      val next = sum + adjusted
      compensation = (next - sum) - adjusted
      sum = next
      index += 1
    }
    requireFinite(functionName, sum)
    sum
  }
}
