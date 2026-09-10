package com.pista.spark.sql.catalyst.functions.aggregate

import com.pista.spark.sql.catalyst.functions.internal.roaring.Roaring64Kernel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}
import org.roaringbitmap.longlong.Roaring64NavigableMap

sealed abstract class Roaring64AggregateBase
  extends TypedImperativeAggregate[Roaring64NavigableMap]
  with UnaryLike[Expression]
  with ImplicitCastInputTypes {
  def child: Expression

  override val dataType: DataType = BinaryType
  override val nullable: Boolean = false
  override def defaultResult: Option[Literal] = Some(Literal(Roaring64Kernel.encode(Roaring64Kernel.empty())))
  override def createAggregationBuffer(): Roaring64NavigableMap = Roaring64Kernel.empty()
  override def merge(
      buffer: Roaring64NavigableMap,
      input: Roaring64NavigableMap): Roaring64NavigableMap = {
    buffer.or(input)
    buffer
  }
  override def eval(buffer: Roaring64NavigableMap): Any = Roaring64Kernel.encode(buffer)
  override def serialize(buffer: Roaring64NavigableMap): Array[Byte] = Roaring64Kernel.encode(buffer)
  override def deserialize(bytes: Array[Byte]): Roaring64NavigableMap =
    Roaring64Kernel.decode(prettyName, bytes)
}

final case class Roaring64Build(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Roaring64AggregateBase {
  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def update(
      buffer: Roaring64NavigableMap,
      input: InternalRow): Roaring64NavigableMap = {
    val value = child.eval(input)
    if (value != null) Roaring64Kernel.addNonNegative(buffer, value.asInstanceOf[Long], prettyName)
    buffer
  }
  override def prettyName: String = "pista_roaring64_build"
  override def withNewMutableAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildInternal(newChild: Expression): Roaring64Build = copy(child = newChild)
}

final case class Roaring64UnionAggregate(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends Roaring64AggregateBase {
  override def inputTypes: Seq[DataType] = Seq(BinaryType)
  override def update(
      buffer: Roaring64NavigableMap,
      input: InternalRow): Roaring64NavigableMap = {
    val value = child.eval(input)
    if (value != null)
      Roaring64Kernel.unionEncoded(buffer, value.asInstanceOf[Array[Byte]], prettyName)
    buffer
  }
  override def prettyName: String = "pista_roaring64_union_agg"
  override def withNewMutableAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = offset)
  override def withNewInputAggBufferOffset(offset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = offset)
  override protected def withNewChildInternal(newChild: Expression): Roaring64UnionAggregate =
    copy(child = newChild)
}
