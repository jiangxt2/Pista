package com.pista.spark.sql.catalyst.functions.aggregate

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.vector.VectorKernel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType}

import java.nio.ByteBuffer

final class VectorCentroidBuffer(
    var count: Long,
    var sums: Array[Double],
    var compensations: Array[Double]) extends Serializable

final case class VectorCentroid(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[VectorCentroidBuffer]
  with UnaryLike[Expression]
  with ImplicitCastInputTypes {
  private val VectorType = ArrayType(DoubleType, containsNull = false)

  override def inputTypes: Seq[DataType] = Seq(VectorType)
  override def dataType: ArrayType = VectorType
  override def nullable: Boolean = true
  override def prettyName: String = "pista_vector_centroid"
  override def createAggregationBuffer(): VectorCentroidBuffer =
    new VectorCentroidBuffer(0L, Array.emptyDoubleArray, Array.emptyDoubleArray)

  override def update(buffer: VectorCentroidBuffer, input: InternalRow): VectorCentroidBuffer = {
    val value = child.eval(input)
    if (value != null) add(buffer, VectorKernel.validated(prettyName, value.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]), 1L)
    buffer
  }

  override def merge(
      buffer: VectorCentroidBuffer,
      input: VectorCentroidBuffer): VectorCentroidBuffer = {
    if (input.count > 0)
      addTotals(
        buffer,
        input.sums,
        input.compensations,
        input.count)
    buffer
  }

  override def eval(buffer: VectorCentroidBuffer): Any =
    if (buffer.count == 0) null
    else new GenericArrayData(buffer.sums.indices.map(index =>
      (buffer.sums(index) + buffer.compensations(index)) / buffer.count.toDouble).toArray)

  override def serialize(buffer: VectorCentroidBuffer): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(12 + buffer.sums.length * 16)
    byteBuffer.putInt(buffer.sums.length)
    byteBuffer.putLong(buffer.count)
    buffer.sums.foreach(byteBuffer.putDouble)
    buffer.compensations.foreach(byteBuffer.putDouble)
    byteBuffer.array()
  }

  override def deserialize(bytes: Array[Byte]): VectorCentroidBuffer = {
    if (bytes.length < 12)
      throw PistaUDFErrors.functionBinaryFormatError(prettyName, "aggregate buffer is truncated")
    val byteBuffer = ByteBuffer.wrap(bytes)
    val dimension = byteBuffer.getInt()
    val count = byteBuffer.getLong()
    if (dimension < 0 || dimension > VectorKernel.MaxDimension || bytes.length != 12 + dimension * 16)
      throw PistaUDFErrors.functionBinaryFormatError(prettyName, "aggregate buffer has invalid length")
    val sums = Array.fill(dimension)(byteBuffer.getDouble())
    val compensations = Array.fill(dimension)(byteBuffer.getDouble())
    new VectorCentroidBuffer(count, sums, compensations)
  }

  override def withNewMutableAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildInternal(newChild: Expression): VectorCentroid = copy(child = newChild)

  private def add(buffer: VectorCentroidBuffer, values: Array[Double], weight: Long): Unit = {
    if (buffer.count == 0) {
      buffer.sums = Array.fill(values.length)(0.0)
      buffer.compensations = Array.fill(values.length)(0.0)
    } else if (buffer.sums.length != values.length) {
      throw PistaUDFErrors.functionInvalidInputError(
        prettyName, s"dimensions differ: ${buffer.sums.length} and ${values.length}")
    }
    buffer.count = Math.addExact(buffer.count, weight)
    var index = 0
    while (index < values.length) {
      val weighted = values(index) * weight.toDouble
      VectorKernel.requireFinite(prettyName, weighted)
      addCompensated(buffer, index, weighted)
      index += 1
    }
  }

  private def addTotals(
      buffer: VectorCentroidBuffer,
      sums: Array[Double],
      compensations: Array[Double],
      count: Long): Unit = {
    if (buffer.count == 0) {
      buffer.sums = Array.fill(sums.length)(0.0)
      buffer.compensations = Array.fill(sums.length)(0.0)
    } else if (buffer.sums.length != sums.length) {
      throw PistaUDFErrors.functionInvalidInputError(
        prettyName, s"dimensions differ: ${buffer.sums.length} and ${sums.length}")
    }
    buffer.count = Math.addExact(buffer.count, count)
    var index = 0
    while (index < sums.length) {
      addCompensated(buffer, index, sums(index))
      addCompensated(buffer, index, compensations(index))
      index += 1
    }
  }

  /** Neumaier compensation is stable when the next value is larger than the running sum. */
  private def addCompensated(
      buffer: VectorCentroidBuffer,
      index: Int,
      value: Double): Unit = {
    VectorKernel.requireFinite(prettyName, value)
    val sum = buffer.sums(index)
    val next = sum + value
    val correction = if (Math.abs(sum) >= Math.abs(value)) (sum - next) + value
    else (value - next) + sum
    buffer.sums(index) = next
    buffer.compensations(index) += correction
    VectorKernel.requireFinite(prettyName, next)
    VectorKernel.requireFinite(prettyName, buffer.compensations(index))
  }
}
