package com.pista.spark.sql.catalyst.functions.catalog

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.types.StructType

sealed trait FunctionKind extends Product with Serializable {
  def id: String
}

object FunctionKind {
  case object Scalar extends FunctionKind { override val id: String = "scalar" }
  case object Aggregate extends FunctionKind { override val id: String = "aggregate" }
  case object Table extends FunctionKind { override val id: String = "table" }
}

sealed trait FunctionCategory extends Product with Serializable {
  def id: String
}

object FunctionCategory {
  case object Network extends FunctionCategory { override val id: String = "network" }
  case object DateTime extends FunctionCategory { override val id: String = "datetime" }
  case object Roaring64 extends FunctionCategory { override val id: String = "roaring64" }
  case object Json extends FunctionCategory { override val id: String = "json" }
  case object UrlDomain extends FunctionCategory { override val id: String = "url-domain" }
  case object Vector extends FunctionCategory { override val id: String = "vector" }
}

final case class FunctionArgument(name: String, dataType: String, foldable: Boolean = false)

final case class FunctionSignature(arguments: Seq[FunctionArgument], returnType: String)

final case class FunctionDocumentation(
    summary: String,
    arguments: String,
    examples: String,
    notes: String = "")

sealed trait FunctionDefinition extends Product with Serializable {
  def canonicalName: String
  def category: FunctionCategory
  def signatures: Seq[FunctionSignature]
  def documentation: FunctionDocumentation
  def since: Option[String]
  def contractDigest: String
  def expressionInfo: ExpressionInfo
  def kind: FunctionKind
}

final case class ScalarFunctionDefinition(
    canonicalName: String,
    category: FunctionCategory,
    signatures: Seq[FunctionSignature],
    documentation: FunctionDocumentation,
    since: Option[String],
    contractDigest: String,
    expressionInfo: ExpressionInfo,
    builder: FunctionRegistry.FunctionBuilder)
  extends FunctionDefinition {
  override val kind: FunctionKind = FunctionKind.Scalar
}

final case class AggregateFunctionDefinition(
    canonicalName: String,
    category: FunctionCategory,
    signatures: Seq[FunctionSignature],
    documentation: FunctionDocumentation,
    since: Option[String],
    contractDigest: String,
    expressionInfo: ExpressionInfo,
    builder: FunctionRegistry.FunctionBuilder)
  extends FunctionDefinition {
  override val kind: FunctionKind = FunctionKind.Aggregate
}

final case class TableFunctionDefinition(
    canonicalName: String,
    category: FunctionCategory,
    signatures: Seq[FunctionSignature],
    outputSchema: StructType,
    documentation: FunctionDocumentation,
    since: Option[String],
    contractDigest: String,
    expressionInfo: ExpressionInfo,
    builder: TableFunctionRegistry.TableFunctionBuilder)
  extends FunctionDefinition {
  override val kind: FunctionKind = FunctionKind.Table
}
