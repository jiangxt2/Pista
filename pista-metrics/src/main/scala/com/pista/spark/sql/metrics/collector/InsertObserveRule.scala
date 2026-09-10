package com.pista.spark.sql.metrics.collector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile, Average, Complete, Count, HyperLogLogPlusPlus, Max, Min, StddevSamp, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, EqualTo, If, Length, Literal, NamedExpression, StringTrim}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CollectMetrics, LocalRelation, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFiles}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.{DoubleType, LongType, NumericType, StringType}

import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

/**
 * INSERT quality\_observation Optimizer\_Rule
 *
 * In the Optimizer stage, identify the write command (V1 DataWritingCommand / V2 WriteCommand),
 * Replace the subquery with a new plan wrapped in CollectMetrics, piggybacked by the INSERT Job.
 * capture data quality metrics with no additional jobs. Job.
 *
 * Registration method:
 * {{{
 *   spark.experimental.extraOptimizations ++= Seq(new InsertObserveRule(maxColumns))
 * }}}
 *
 * @param maxColumns Maximum number of columns for statistics (columns with numerical values are prioritized for retention when exceeded)
 * @param numericPercentilesEnabled Whether numerical percentiles are enabled
 * @param numericPercentilePoints Numeric percentile points (e.g., 0.5, 0.95, 0.99)
 *
 */
class InsertObserveRule(
  maxColumns: Int,
  numericPercentilesEnabled: Boolean = true,
  numericPercentilePoints: Seq[Double] = Seq(0.5, 0.95, 0.99)
) extends Rule[LogicalPlan] with Logging {

  private val idGen = new AtomicLong(0L)
  private val MARKER = "__pista_insert__"
  private val StringLengthPercentiles: Array[Double] = Array(0.5d, 0.95d)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val result = plan transformDown {

      // V1 (whitelist): Hive INSERT
      case cmd: InsertIntoHiveTable =>
        injectIntoV1Command(cmd, "V1 InsertIntoHiveTable")

      // V1 (whitelist): HadoopFS INSERT
      case cmd: InsertIntoHadoopFsRelationCommand =>
        injectIntoV1Command(cmd, "V1 InsertIntoHadoopFsRelationCommand")

      // V2:AppendData(INSERT INTO on Iceberg/Delta)
      case cmd: AppendData =>
        buildObservedChild("V2 AppendData", cmd.query)
          .map(observed => cmd.copy(query = observed))
          .getOrElse(cmd)

      // V2:OverwriteByExpression(INSERT OVERWRITE with static partition)
      case cmd: OverwriteByExpression =>
        buildObservedChild("V2 OverwriteByExpression", cmd.query)
          .map(observed => cmd.copy(query = observed))
          .getOrElse(cmd)

      // V2:OverwritePartitionsDynamic(INSERT OVERWRITE with dynamic partition)
      case cmd: OverwritePartitionsDynamic =>
        buildObservedChild("V2 OverwritePartitionsDynamic", cmd.query)
          .map(observed => cmd.copy(query = observed))
          .getOrElse(cmd)
    }
    result
  }

  private def injectIntoV1Command(cmd: DataWritingCommand, source: String): LogicalPlan =
    cmd.child match {
      // Spark planned write requirement WriteFiles must be a write command child root node, cannot be enclosed by CollectMetrics enveloped
      case wf: WriteFiles =>
        buildObservedChild(s"$source.WriteFilesChild", wf.child) match {
          case Some(observedChild) =>
            cmd.withNewChildren(Seq(wf.copy(child = observedChild)))
          case None =>
            cmd
        }
      case child =>
        buildObservedChild(source, child) match {
          case Some(observed) =>
            cmd.withNewChildren(Seq(observed))
          case None =>
            cmd
        }
    }

  private def buildObservedChild(source: String, child: LogicalPlan): Option[CollectMetrics] = {
    if (hasMarker(child)) {
      logInfo(s"[InsertObserveRule] Skip observe for $source: marker already exists")
      None
    } else if (!child.resolved || child.output.exists(!_.resolved)) {
      logInfo(s"[InsertObserveRule] Skip observe for $source: unresolved child/output detected")
      None
    } else if (isValuesLikeChild(child)) {
      logInfo(s"[InsertObserveRule] Skip observe for $source: VALUES(LocalRelation) is not covered in current version")
      None
    } else
      try {
        val aggExprs = buildAggregateExprs(child.output, maxColumns)
        if (aggExprs.isEmpty) {
          logInfo(s"[InsertObserveRule] Skip observe for $source: empty metric expressions")
          None
        } else {
          val (resolvedAggExprs, unresolvedAggExprs) = aggExprs.partition(_.resolved)
          if (resolvedAggExprs.isEmpty) {
            logInfo(s"[InsertObserveRule] Skip observe for $source: all metric expressions unresolved")
            None
          } else {
            if (unresolvedAggExprs.nonEmpty) {
              val unresolvedNames = unresolvedAggExprs.map(_.name).mkString(",")
              logWarning(
                s"[InsertObserveRule] Drop unresolved metric expressions for $source: " +
                  s"count=${unresolvedAggExprs.size}, names=$unresolvedNames"
              )
            }
            logInfo(
              s"[InsertObserveRule] Injecting observe into $source with " +
                s"${resolvedAggExprs.size}/${aggExprs.size} metric expressions"
            )
            Some(CollectMetrics(MARKER, resolvedAggExprs, child, idGen.incrementAndGet()))
          }
        }
      } catch {
        case NonFatal(e) =>
          // Failures in data collection should not affect the main process execution.
          logInfo(s"[InsertObserveRule] Skipping observation for an unsupported plan (${e.getClass.getSimpleName})")
          None
      }
  }

  private def isValuesLikeChild(plan: LogicalPlan): Boolean = {
    val leaves = plan.collectLeaves()
    leaves.nonEmpty && leaves.forall(_.isInstanceOf[LocalRelation])
  }

  /**
   * parsed output attributes to construct aggregate expressions output Construct aggregation expressions by the parsed output attribute
   *
   * Note: Rule runs during the Optimizer phase and cannot rely on unanalyzed expressions generated by the functions API.
   * Builds resolved Catalyst AggregateExpression nodes so every reference binds to child.output.
   */
  private def buildAggregateExprs(
    output: Seq[Attribute],
    maxColumns: Int
  ): Seq[NamedExpression] = {
    // Truncate: Column of numeric values first
    val numericAttrs = output.filter(_.dataType.isInstanceOf[NumericType])
    val stringAttrs = output.filter(_.dataType == StringType)
    val otherAttrs = output.filterNot(_.dataType.isInstanceOf[NumericType])
    val targetAttrs = (numericAttrs ++ otherAttrs).take(maxColumns)
    val numericSet = numericAttrs.map(_.exprId).toSet
    val stringSet = stringAttrs.map(_.exprId).toSet

    // Sanitize columnName
    val colAliases = buildAliases(targetAttrs.map(_.name))

    val exprs = scala.collection.mutable.ArrayBuffer[NamedExpression]()

    // 1. Number of rows
    exprs += Alias(
      AggregateExpression(Count(Seq(Literal(1))), Complete, isDistinct = false, filter = None),
      "_total"
    )()

    // 2. Column non-null count + extension statistics for string/numeric columns
    targetAttrs.foreach { attr =>
      val alias = colAliases(attr.name)
      val isNumeric = numericSet.contains(attr.exprId)
      val isString = stringSet.contains(attr.exprId)

      // NonEmpty Count
      exprs += Alias(
        AggregateExpression(Count(attr), Complete, isDistinct = false, filter = None),
        s"_nn_$alias"
      )()

      // Approximate distinct count (HyperLogLog++, relativeSD=0.05)
      exprs += Alias(
        AggregateExpression(
          new HyperLogLogPlusPlus(attr, 0.05, 0, 0),
          Complete, isDistinct = false, filter = None
        ),
        s"_hll_$alias"
      )()

      // Column quality statistics
      if (isString) {
        exprs += Alias(
          Cast(
            AggregateExpression(
              Sum(If(EqualTo(attr, Literal("")), Literal(1L), Literal(0L))),
              Complete,
              isDistinct = false,
              filter = None
            ),
            LongType
          ),
          s"_empty_$alias"
        )()
        exprs += Alias(
          Cast(
            AggregateExpression(
              Sum(If(EqualTo(StringTrim(attr), Literal("")), Literal(1L), Literal(0L))),
              Complete,
              isDistinct = false,
              filter = None
            ),
            LongType
          ),
          s"_blank_$alias"
        )()
        exprs += Alias(
          AggregateExpression(
            new ApproximatePercentile(
              Length(attr),
              Literal(StringLengthPercentiles),
              Literal(10000),
              0, 0
            ),
            Complete, isDistinct = false, filter = None
          ),
          s"_len_pct_$alias"
        )()
      }

      // Additional statistics for numeric columns
      if (isNumeric) {
        exprs += Alias(
          AggregateExpression(Min(attr), Complete, isDistinct = false, filter = None),
          s"_min_$alias"
        )()
        exprs += Alias(
          AggregateExpression(Max(attr), Complete, isDistinct = false, filter = None),
          s"_max_$alias"
        )()
        exprs += Alias(
          Cast(AggregateExpression(Average(attr), Complete, isDistinct = false, filter = None), DoubleType),
          s"_avg_$alias"
        )()
        exprs += Alias(
          AggregateExpression(StddevSamp(attr), Complete, isDistinct = false, filter = None),
          s"_std_$alias"
        )()

        if (numericPercentilesEnabled && numericPercentilePoints.nonEmpty) {
          exprs += Alias(
            AggregateExpression(
              new ApproximatePercentile(
                attr,
                Literal(numericPercentilePoints.toArray),
                Literal(10000),
                0, 0
              ),
              Complete, isDistinct = false, filter = None
            ),
            s"_pct_$alias"
          )()
        }
      }
    }

    exprs
  }

  /**
   * column sanitize + collision removal
   */
  private def buildAliases(columns: Seq[String]): Map[String, String] = {
    val sanitized = columns.map(c => c -> c.replaceAll("[^a-zA-Z0-9_]", "_"))
    val seen = scala.collection.mutable.Map[String, Int]()
    sanitized.map { case (orig, clean) =>
      val finalAlias = if (seen.contains(clean)) {
        val idx = seen(clean)
        seen(clean) = idx + 1
        s"${clean}_$idx"
      } else {
        seen(clean) = 1
        clean
      }
      orig -> finalAlias
    }.toMap
  }

  private def hasMarker(plan: LogicalPlan): Boolean =
    plan.exists {
      case c: CollectMetrics => c.name == MARKER
      case _ => false
    }
}
