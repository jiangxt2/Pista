package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.model.Metrics
import com.pista.spark.sql.metrics.writer.MetricsQueue
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.util.QueryExecutionListener

import scala.util.control.NonFatal

/**
 * INSERT quality metrics listener
 *
 * Listens to QueryExecutionListener.onSuccess and extracts matching observed metrics.
 * "__pista_insert__" marker, reprocess ObserveMeta from qe.analyzed and enqueue metrics after parsing.
 *
 * Rule → Listener Metadata Passing Mechanism:
 * Listener and Rule hold the same instance of extractor and maxColumns parameter.
 * during the onSuccess of the write command for the child schema of qe.analyzed onSuccess when qe.analyzed the write command child schema reinvoke
 * buildObserveExprs(schema, maxColumns, numericPercentiles...), since this method is a pure function,
 * Column aliases match the injected rule, so the listener needs no shared mutable state.
 *
 * @param extractor DataQualityExtractor instance (shared with InsertObserveRule)
 * @param maxColumns Maximum number of columns for statistics (must be consistent with InsertObserveRule)
 * @param numericPercentilesEnabled whether numerical percentiles are enabled
 * @param numericPercentilePoints Numeric percentile points configuration (e.g., 0.5,0.95,0.99)
 * @param appId      Application ID
 * @param queue      Metric Queue
 *
 */
class InsertMetricsListener(
  extractor: DataQualityExtractor,
  maxColumns: Int,
  numericPercentilesEnabled: Boolean,
  numericPercentilePoints: Seq[Double],
  appId: String,
  queue: MetricsQueue[Metrics]
) extends QueryExecutionListener with Logging {

  private case class WriteInfo(childSchema: StructType, tableName: String, sourceType: String)

  // funcName and the corresponding trigger path relationship:
  //   "command"           : sparkSession.sql("INSERT ...").eagerlyExecuteCommands()
  //   "append"/"overwrite": DataFrameWriter.insertInto() / overwrite()
  //   "overwritePartitions": DataFrameWriterV2.overwritePartitions()(OverwritePartitionsDynamic)
  //   "create"/"insertInto": other V2 write command
  private val WRITE_FUNC_NAMES =
    Set("command", "append", "overwrite", "overwritePartitions", "create", "insertInto")

  private val MARKER = "__pista_insert__"

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val hasMarker = qe.observedMetrics.contains(MARKER)
    logInfo(s"[InsertMetricsListener] onSuccess called: funcName=$funcName, hasMarker=$hasMarker")
    if (!WRITE_FUNC_NAMES.contains(funcName)) return
    if (!hasMarker) {
      logSkipIfUnsupported(qe.analyzed)
      return
    }
    val writeInfoOpt = extractWriteInfo(qe.analyzed)
    if (writeInfoOpt.isEmpty) {
      logInfo(s"[InsertMetricsListener] Skip parsing observed metrics: unsupported analyzed plan ${qe.analyzed.getClass.getSimpleName}")
      return
    }
    val writeInfo = writeInfoOpt.get

    qe.observedMetrics.get(MARKER).foreach { row =>
      try {
        // Re-push ObserveMeta (Deterministic: pure function) with same parameters
        val (_, metaTemplate) = extractor.buildObserveExprs(
          schema = writeInfo.childSchema,
          maxColumns = maxColumns,
          numericPercentilesEnabled = numericPercentilesEnabled,
          numericPercentilePoints = numericPercentilePoints
        )
        val meta = metaTemplate.copy(
          appId       = appId,
          executionId = qe.id,
          tableName   = writeInfo.tableName
        )
        val rowMap   = rowToTypedValuesMap(row)
        val metrics  = extractor.parseFromObservation(rowMap, meta)
          .copy(metricType = "insert_data_quality", sourceType = Some(writeInfo.sourceType))
        queue.offer(metrics)
        logInfo(s"[InsertMetricsListener] Collection completed: table=${writeInfo.tableName}, " +
          s"totalRecords=${metrics.totalRecords}, executionId=${qe.id}")
      } catch {
        case e: Exception =>
          logWarning(s"[InsertMetricsListener] Column parsing failed with ${e.getClass.getSimpleName}; skipping")
      }
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()

  /**
   * Uses the row schema to convert ArrayData values into typed Seq values.
   * Avoid losing element type information during subsequent parsing.
   */
  private def rowToTypedValuesMap(row: org.apache.spark.sql.Row): Map[String, Any] =
    row.schema.fields.zipWithIndex.map { case (field, idx) =>
      val raw = if (row.isNullAt(idx)) null else row.get(idx)
      field.name -> normalizeValue(raw, field.dataType)
    }.toMap

  private def normalizeValue(value: Any, dataType: DataType): Any =
    (value, dataType) match {
      case (null, _) => null
      case (arr: ArrayData, ArrayType(elemType, _)) =>
        try arr.toSeq[Any](elemType)
        catch {
          case NonFatal(_) => value
        }
      case _ => value
    }

  /**
   * Extract the child schema of the write command from the analyzed plan, including the target table name and engine type.
   * If the write command cannot be recognized, return null (convergence marked observedMetrics but plan match fails is an anomaly)
   */
  private def extractWriteInfo(plan: LogicalPlan): Option[WriteInfo] =
    plan match {
      case cmd: DataWritingCommand =>
        val tableName  = resolveV1TableName(cmd)
        val sourceType = if (cmd.getClass.getName.contains("Hive")) "v1_hive" else "v1_hadoopfs"
        Some(WriteInfo(cmd.child.schema, tableName, sourceType))
      case cmd: AppendData =>
        Some(WriteInfo(cmd.query.schema, resolveV2TableName(cmd.table), "v2"))
      case cmd: OverwriteByExpression =>
        Some(WriteInfo(cmd.query.schema, resolveV2TableName(cmd.table), "v2"))
      case cmd: OverwritePartitionsDynamic =>
        Some(WriteInfo(cmd.query.schema, resolveV2TableName(cmd.table), "v2"))
      case other =>
        logInfo(s"[InsertMetricsListener] Skip unsupported write logical plan: ${other.getClass.getSimpleName}")
        None
    }

  private def logSkipIfUnsupported(plan: LogicalPlan): Unit =
    plan match {
      case _: CreateHiveTableAsSelectCommand | _: CreateDataSourceTableAsSelectCommand =>
        logInfo(s"[InsertMetricsListener] Skip INSERT DQ for CTAS command: ${plan.getClass.getSimpleName}")
      case _ =>
        ()
    }

  private def resolveV2TableName(table: NamedRelation): String =
    table match {
      case rel: DataSourceV2Relation =>
        val ident = rel.identifier.map(_.toString).filter(_.nonEmpty)
        (rel.catalog, ident) match {
          case (Some(catalog), Some(identifier)) => s"${catalog.name()}.$identifier"
          case (None, Some(identifier)) => identifier
          case _ => Option(rel.name).filter(_.nonEmpty).getOrElse(rel.getClass.getSimpleName)
        }
      case _ =>
        Option(table.name).filter(_.nonEmpty).getOrElse(table.getClass.getSimpleName)
    }

  private def resolveV1TableName(cmd: DataWritingCommand): String =
    cmd match {
      case hiveCmd: InsertIntoHiveTable =>
        hiveCmd.table.identifier.unquotedString
      case fsCmd: InsertIntoHadoopFsRelationCommand =>
        fsCmd.catalogTable
          .map(_.identifier.unquotedString)
          .getOrElse(s"path:${fsCmd.outputPath.toString}")
      case _ =>
        cmd.getClass.getSimpleName
    }
}
