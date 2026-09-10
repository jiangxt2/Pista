package com.pista.spark.sql.functions

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.catalog._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

import scala.collection.mutable.ArrayBuffer

object PistaFunctionInstaller {
  def install(session: SparkSession): FunctionInstallationReport = synchronized {
    val functionRegistry = session.sessionState.functionRegistry
    val tableFunctionRegistry = session.sessionState.tableFunctionRegistry
    val alreadyPresent = ArrayBuffer.empty[String]

    PistaFunctionCatalog.definitions.foreach { definition =>
      val identifier = FunctionIdentifier(definition.canonicalName)
      val (existing, crossRegistryConflict) = lookupRegistrations(
        definition, identifier, functionRegistry, tableFunctionRegistry)
      crossRegistryConflict.foreach { info =>
        throw PistaUDFErrors.functionCatalogConflictError(
          definition.canonicalName,
          definition.expressionInfo.getClassName,
          s"other-registry:${owner(info)}")
      }
      existing.foreach { info =>
        if (info.getClassName != definition.expressionInfo.getClassName)
          throw PistaUDFErrors.functionCatalogConflictError(
            definition.canonicalName,
            definition.expressionInfo.getClassName,
            owner(info))
        alreadyPresent += definition.canonicalName
      }
    }

    val installed = ArrayBuffer.empty[(FunctionDefinition, FunctionIdentifier)]
    try {
      PistaFunctionCatalog.definitions.foreach { definition =>
        if (!alreadyPresent.contains(definition.canonicalName)) {
          val identifier = FunctionIdentifier(definition.canonicalName)
          definition match {
            case scalar: ScalarFunctionDefinition =>
              functionRegistry.registerFunction(identifier, scalar.expressionInfo, scalar.builder)
            case aggregate: AggregateFunctionDefinition =>
              functionRegistry.registerFunction(identifier, aggregate.expressionInfo, aggregate.builder)
            case table: TableFunctionDefinition =>
              tableFunctionRegistry.registerFunction(identifier, table.expressionInfo, table.builder)
          }
          installed += definition -> identifier
        }
      }
    } catch {
      case exception: Throwable =>
        val rollbackFailures = installed.reverseIterator.flatMap { case (definition, identifier) =>
          val dropped = definition match {
            case _: TableFunctionDefinition => tableFunctionRegistry.dropFunction(identifier)
            case _ => functionRegistry.dropFunction(identifier)
          }
          if (dropped) None else Some(definition.canonicalName)
        }.toVector
        val suffix = if (rollbackFailures.isEmpty) "all partial registrations were rolled back"
        else s"rollback failed for ${rollbackFailures.mkString(",")}"
        throw PistaUDFErrors.functionInstallationError(
          s"catalog ${PistaFunctionCatalog.digest} could not be installed; $suffix", exception)
    }

    FunctionInstallationReport(
      PistaFunctionCatalog.digest,
      installed.iterator.map(_._1.canonicalName).toVector,
      alreadyPresent.sorted.toVector)
  }

  def validate(session: SparkSession): Unit = {
    val functionRegistry = session.sessionState.functionRegistry
    val tableFunctionRegistry = session.sessionState.tableFunctionRegistry
    PistaFunctionCatalog.definitions.foreach { definition =>
      val identifier = FunctionIdentifier(definition.canonicalName)
      val (actual, crossRegistryConflict) = lookupRegistrations(
        definition, identifier, functionRegistry, tableFunctionRegistry)
      crossRegistryConflict.foreach { info =>
        throw PistaUDFErrors.functionCatalogConflictError(
          definition.canonicalName,
          definition.expressionInfo.getClassName,
          s"other-registry:${owner(info)}")
      }
      if (actual.forall(_.getClassName != definition.expressionInfo.getClassName))
        throw PistaUDFErrors.functionCatalogConflictError(
          definition.canonicalName,
          definition.expressionInfo.getClassName,
          actual.map(owner).getOrElse("missing"))
    }
  }

  private def lookupRegistrations(
      definition: FunctionDefinition,
      identifier: FunctionIdentifier,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry
  ): (Option[ExpressionInfo], Option[ExpressionInfo]) = definition match {
    case _: TableFunctionDefinition =>
      tableFunctionRegistry.lookupFunction(identifier) ->
        functionRegistry.lookupFunction(identifier)
    case _ =>
      functionRegistry.lookupFunction(identifier) ->
        tableFunctionRegistry.lookupFunction(identifier)
  }

  private def owner(info: ExpressionInfo): String =
    Option(info.getClassName).getOrElse("unknown")
}
