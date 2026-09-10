package com.pista.spark.sql.functions

import com.pista.spark.sql.catalyst.functions.catalog._
import org.apache.spark.sql.{SparkSessionExtensions, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.FunctionIdentifier

final class PistaSparkSessionExtensions extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    val pistaNames = PistaFunctionCatalog.definitions
      .map(definition => FunctionIdentifier(definition.canonicalName)).toSet
    val sparkBuiltinNames = org.apache.spark.sql.catalyst.analysis.FunctionRegistry.functionSet ++
      org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.functionSet
    require(
      pistaNames.intersect(sparkBuiltinNames).isEmpty,
      "Pista functions must not override Spark built-in scalar, aggregate, or table functions")

    PistaFunctionCatalog.definitions.foreach {
      case scalar: ScalarFunctionDefinition =>
        extensions.injectFunction((FunctionIdentifier(scalar.canonicalName), scalar.expressionInfo, scalar.builder))
      case aggregate: AggregateFunctionDefinition =>
        extensions.injectFunction((FunctionIdentifier(aggregate.canonicalName), aggregate.expressionInfo, aggregate.builder))
      case table: TableFunctionDefinition =>
        extensions.injectTableFunction((FunctionIdentifier(table.canonicalName), table.expressionInfo, table.builder))
    }
    extensions.injectCheckRule(session => _ => PistaFunctionInstaller.validate(session))
  }
}
