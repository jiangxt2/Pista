package com.pista.spark.sql.catalyst.functions.table

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.roaring.Roaring64Kernel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, OneRowRelation}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, IntegerType, LongType, StructField, StructType}

final case class Roaring64Explode(bitmap: Expression, maxRows: Expression)
  extends CollectionGenerator with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  private lazy val invocation = StaticInvoke(
    Roaring64Kernel.getClass,
    ArrayType(LongType, containsNull = false),
    "explode",
    Seq(bitmap, maxRows),
    Seq(BinaryType, IntegerType),
    propagateNull = true,
    returnNullable = true)

  override def children: Seq[Expression] = Seq(bitmap, maxRows)
  override def elementSchema: StructType = StructType(Seq(StructField("value", LongType, nullable = false)))
  override def position: Boolean = false
  override def inline: Boolean = false
  override def collectionType: ArrayType = ArrayType(LongType, containsNull = false)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val values = invocation.eval(input).asInstanceOf[ArrayData]
    if (values == null) Nil
    else (0 until values.numElements()).iterator.map(index => InternalRow(values.getLong(index)))
  }
  override protected def doGenCode(context: CodegenContext, code: ExprCode): ExprCode =
    invocation.genCode(context)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Roaring64Explode = copy(newChildren(0), newChildren(1))
}

object Roaring64Explode {
  val builder: TableFunctionRegistry.TableFunctionBuilder = children => {
    if (children.length != 2)
      throw PistaUDFErrors.udfArgumentLengthMismatchError(
        "pista_roaring64_explode", 2, children.length)
    if (children.head.dataType != BinaryType || children(1).dataType != IntegerType)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_roaring64_explode", "bitmap must be BINARY and max_rows must be INT")
    if (!children(1).foldable)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_roaring64_explode", "max_rows must be foldable")
    val maxRows = try children(1).eval() catch {
      case _: Exception =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_roaring64_explode", "max_rows must be a positive integer")
    }
    maxRows match {
      case value: java.lang.Number if value.longValue() > 0L =>
      case _ =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_roaring64_explode", "max_rows must be a non-null positive integer")
    }
    Generate(
      Roaring64Explode(children.head, children(1)),
      unrequiredChildIndex = Nil,
      outer = false,
      qualifier = None,
      generatorOutput = Nil,
      child = OneRowRelation())
  }
}
