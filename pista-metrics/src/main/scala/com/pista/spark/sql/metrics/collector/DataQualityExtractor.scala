package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.model.{ColumnQualityStats, DataQualityMetrics, Metrics}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Column, functions => F}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

import scala.collection.JavaConverters._

/**
 * observe() Aggregates metadata
 *
 * Defines the field names shared by observation construction and result parsing.
 * observation.get result, to avoid positional coupling.
 *
 * @param targetColumns All columns involved in the statistics (original column names)
 * @param numericColumns Collection of numeric columns (for min/max/avg/stddev)
 * @param stringColumns Collection of string columns (for empty/blank/lengthP50/lengthP95)
 * @param colAliases     Original column names -> suffixes used in the alias expressions in aggregation (which may differ after sanitization)
 * @param numericPercentilePoints Numeric percentile points configuration (e.g., 0.5, 0.95, 0.99)
 * @param appId          Application ID (passed to DataQualityMetrics)
 * @param executionId    Execution ID
 * @param tableName      Table name
 * @param sqlIndex       SQL Index Identifier (e.g., sql_1)
 */
private[metrics] case class ObserveMeta(
  targetColumns: Seq[String],
  numericColumns: Set[String],
  stringColumns: Set[String],
  colAliases: Map[String, String],
  numericPercentilePoints: Seq[Double],
  appId: String,
  executionId: Long,
  tableName: String,
  sqlIndex: Option[String]
)

/**
 * Data quality extractor
 *
 * Analyze DataFrame quality metrics, including per-column statistics:
 * - For all columns: nullCount/nullRationullCount / nullRatio
 * - additional numerical columns: min value / max value / average value / standard deviation valueminValue / maxValue / avgValue / stdDevValue
 *
 */
class DataQualityExtractor extends Logging {

  /**
   * Build the list of aggregation expressions required for the observe() function (SELECT path)
   *
   * Expression layout corresponds to parseFromObservation:
   * - _total              :row count
   * - _nn_$col            : number of non-empty rows per column
   * - _min_$col / _max_$col / _avg_$col / _std_$col: Numerical Column Statistics
   *
   * Numeric columns are identified with isInstanceOf[NumericType], covering every Spark numeric type.
   * (ByteType/ShortType/IntegerType/LongType/FloatType/DoubleType/DecimalType).
   * min/max preserve original column types, avg/stddev unify to double.
   *
   * Column truncation strategy aligns with the INSERT path (priority for numeric columns), and SELECT path does not sanitize column names.
   */
  def buildObserveExprs(
    df: DataFrame,
    appId: String,
    executionId: Long,
    tableName: String,
    sqlIndex: Option[String] = None,
    maxColumns: Int = 50,
    numericPercentilesEnabled: Boolean = true,
    numericPercentilePoints: Seq[Double] = Seq(0.5, 0.95, 0.99)
  ): (Seq[Column], ObserveMeta) = {
    val allColumns     = df.columns.toSeq
    val numericAll     = df.schema.fields
      .filter(f => f.dataType.isInstanceOf[NumericType])
      .map(_.name)
      .toSet
    val stringAll      = df.schema.fields
      .filter(_.dataType == StringType)
      .map(_.name)
      .toSet
    // Truncate: Column with numeric values prioritized, consistent with the INSERT path.
    val numericFirst   = allColumns.filter(numericAll.contains)
    val others         = allColumns.filterNot(numericAll.contains)
    val targetColumns  = (numericFirst ++ others).take(maxColumns)
    val numericColumns = targetColumns.filter(numericAll.contains).toSet
    val stringColumns  = targetColumns.filter(stringAll.contains).toSet
    val percentilePoints = if (numericPercentilesEnabled) numericPercentilePoints else Seq.empty
    // The SELECT path does not sanitize names, so aliases match the original names.
    val colAliases = targetColumns.map(c => c -> c).toMap
    val meta = ObserveMeta(
      targetColumns, numericColumns, stringColumns, colAliases, percentilePoints,
      appId, executionId, tableName, sqlIndex
    )
    (buildExprs(targetColumns, numericColumns, stringColumns, colAliases, percentilePoints), meta)
  }

  /**
   * Build the list of aggregation expressions required for the observe() function (INSERT path, StructType overload)
   *
   * Overload additional support for DataFrame:
   * - Column truncation (maxColumns, prefer to retain numerical columns first)
   * - column names sanitizereplace column names containing special characters with underscores, and append _$idx in case of collisions. _$idx suffix )
   *
   * Return placeholders for appId/executionId/tableName as null in the ObserveMeta.
   * Caller (InsertMetricsListener) copies with a realistic context.
   */
  def buildObserveExprs(
    schema: StructType,
    maxColumns: Int,
    numericPercentilesEnabled: Boolean,
    numericPercentilePoints: Seq[Double]
  ): (Seq[Column], ObserveMeta) = {
    val allColumns     = schema.fields.map(_.name).toSeq
    val numericAll     = schema.fields.filter(_.dataType.isInstanceOf[NumericType]).map(_.name).toSet
    val stringAll      = schema.fields.filter(_.dataType == StringType).map(_.name).toSet

    // Truncate: prefer numeric columns, fill remaining in original order with maxColumns
    val numericFirst   = allColumns.filter(numericAll.contains)
    val others         = allColumns.filterNot(numericAll.contains)
    val targetColumns  = (numericFirst ++ others).take(maxColumns)
    val numericColumns = targetColumns.filter(numericAll.contains).toSet
    val stringColumns  = targetColumns.filter(stringAll.contains).toSet
    val percentilePoints = if (numericPercentilesEnabled) numericPercentilePoints else Seq.empty

    // Sanitize + DISTINCT Collision Deduplication
    val colAliases = buildAliases(targetColumns)

    val meta = ObserveMeta(
      targetColumns, numericColumns, stringColumns, colAliases, percentilePoints, "", 0L, "", None
    )
    (buildExprs(targetColumns, numericColumns, stringColumns, colAliases, percentilePoints), meta)
  }

  /**
   * Parse DataQualityMetrics from the observation.get result (callback after the Action completes)
   *
   * Uses meta.colAliases to find aggregate-result keys for sanitized and unsanitized paths.
   */
  def parseFromObservation(
    result: Map[String, Any],
    meta: ObserveMeta
  ): DataQualityMetrics = {
    val totalRecords = result.get("_total").collect { case n: Number => n.longValue() }.getOrElse(0L)

    def toAnyOpt(key: String): Option[Any] =
      result.get(key).flatMap(Option(_))

    def toLongOpt(key: String): Option[Long] =
      toAnyOpt(key).collect { case n: Number => n.longValue() }

    def toDoubleOpt(key: String): Option[Double] =
      result.get(key).flatMap(v => Option(v).map { case n: Number => n.doubleValue() })

    def toAnySeqOpt(key: String): Option[Seq[Any]] =
      toAnyOpt(key).flatMap {
        case seq: Seq[_] if seq.nonEmpty =>
          Some(seq.map(_.asInstanceOf[Any]))
        case arr: Array[_] if arr.nonEmpty =>
          Some(arr.toSeq.map(_.asInstanceOf[Any]))
        case list: java.util.List[_] if !list.isEmpty =>
          Some(list.asScala.toSeq.map(_.asInstanceOf[Any]))
        case arr: ArrayData =>
          toAnySeqFromArrayData(arr)
        case n: Number => Some(Seq(n))
        case v if v != null => Some(Seq(v))
        case _ => None
      }

    def byIndex(values: Option[Seq[Any]], idx: Int): Option[Any] =
      values.flatMap(_.lift(idx))

    def toLongByIndex(values: Option[Seq[Any]], idx: Int): Option[Long] =
      byIndex(values, idx).collect { case n: Number => n.longValue() }

    def byPercentile(values: Option[Seq[Any]], points: Seq[Double], target: Double): Option[Any] = {
      val idx = points.indexWhere(p => math.abs(p - target) < 1e-9)
      if (idx >= 0) byIndex(values, idx) else None
    }

    val skippedColumns = scala.collection.mutable.ArrayBuffer[String]()
    val columnStats: Map[String, ColumnQualityStats] = meta.targetColumns.flatMap { c =>
      val alias           = meta.colAliases(c)
      val nonNullCountOpt = toLongOpt(s"_nn_$alias")
      if (nonNullCountOpt.isEmpty) {
        skippedColumns += c
        None
      } else {
        val nonNullCount      = nonNullCountOpt.get
        val nullCount         = totalRecords - nonNullCount
        val colNullRatio      = if (totalRecords > 0) nullCount.toDouble / totalRecords else 0.0
        val approxDistinct    = toAnyOpt(s"_hll_$alias").collect { case n: Number => n.longValue() }
        val lengthPercentiles = toAnySeqOpt(s"_len_pct_$alias")
        val numericPercentiles = toAnySeqOpt(s"_pct_$alias")
        val stats = ColumnQualityStats(
          nullCount = nullCount,
          nullRatio = colNullRatio,
          approxDistinctCount = approxDistinct,
          emptyStringCount = if (meta.stringColumns.contains(c)) toLongOpt(s"_empty_$alias") else None,
          blankStringCount = if (meta.stringColumns.contains(c)) toLongOpt(s"_blank_$alias") else None,
          lengthP50 = if (meta.stringColumns.contains(c)) toLongByIndex(lengthPercentiles, 0) else None,
          lengthP95 = if (meta.stringColumns.contains(c)) toLongByIndex(lengthPercentiles, 1) else None,
          minValue = if (meta.numericColumns.contains(c)) toAnyOpt(s"_min_$alias") else None,
          maxValue = if (meta.numericColumns.contains(c)) toAnyOpt(s"_max_$alias") else None,
          avgValue = if (meta.numericColumns.contains(c)) toDoubleOpt(s"_avg_$alias") else None,
          stdDevValue = if (meta.numericColumns.contains(c)) toDoubleOpt(s"_std_$alias") else None,
          p50Value = if (meta.numericColumns.contains(c)) {
            byPercentile(numericPercentiles, meta.numericPercentilePoints, 0.5)
          } else None,
          p95Value = if (meta.numericColumns.contains(c)) {
            byPercentile(numericPercentiles, meta.numericPercentilePoints, 0.95)
          } else None,
          p99Value = if (meta.numericColumns.contains(c)) {
            byPercentile(numericPercentiles, meta.numericPercentilePoints, 0.99)
          } else None
        )
        Some(c -> stats)
      }
    }.toMap

    if (skippedColumns.nonEmpty) {
      logWarning(
        s"[DataQualityExtractor] Skip columns with missing observe keys: ${skippedColumns.mkString(",")}"
      )
    }

    DataQualityMetrics(
      appId        = meta.appId,
      executionId  = meta.executionId,
      tableName    = meta.tableName,
      sqlIndex     = meta.sqlIndex,
      totalRecords = totalRecords,
      columnStats  = columnStats,
      collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
    )
  }

  /**
   * Extract a sequence of elements from ArrayData securely.
   *
   * Explanation:
   * - observation in different execution paths, base arrays of different numeric types (Int/Long/Double/) may be returned.Int/Long/Double/...)
   * - Here, try sequentially by common numerical types. Any mismatch is swallowed and the process continues without throwing an exception.
   * - Return None upon final failure, handled by upper layers for missing fields to avoid discarding entire metric.
   */
  private def toAnySeqFromArrayData(arr: ArrayData): Option[Seq[Any]] = {
    arr match {
      case g: GenericArrayData if g.array.nonEmpty =>
        Some(g.array.toSeq.map(_.asInstanceOf[Any]))
      case _ =>
        None
    }
  }

  // ── Private Utility Method ───────────────────────────────────────────────────

  private def buildExprs(
    targetColumns: Seq[String],
    numericColumns: Set[String],
    stringColumns: Set[String],
    colAliases: Map[String, String],
    numericPercentilePoints: Seq[Double]
  ): Seq[Column] = {
    val exprs = scala.collection.mutable.ArrayBuffer[Column]()
    exprs += F.count(F.lit(1)).as("_total")
    targetColumns.foreach { c =>
      val a = colAliases(c)
      exprs += count(col(s"`$c`")).as(s"_nn_$a")
      exprs += F.approx_count_distinct(col(s"`$c`"), 0.05).as(s"_hll_$a")
      if (stringColumns.contains(c)) {
        exprs += F.count(F.when(col(s"`$c`") === "", F.lit(1))).as(s"_empty_$a")
        exprs += F.count(F.when(F.trim(col(s"`$c`")) === "", F.lit(1))).as(s"_blank_$a")
        exprs += F.percentile_approx(
          F.length(col(s"`$c`")),
          F.array(F.lit(0.5d), F.lit(0.95d)),
          F.lit(10000)
        ).as(s"_len_pct_$a")
      }
    }
    numericColumns.foreach { c =>
      val a = colAliases(c)
      exprs += F.min(col(s"`$c`")).as(s"_min_$a")
      exprs += F.max(col(s"`$c`")).as(s"_max_$a")
      exprs += F.avg(col(s"`$c`").cast("double")).as(s"_avg_$a")
      exprs += F.stddev(col(s"`$c`").cast("double")).as(s"_std_$a")
      if (numericPercentilePoints.nonEmpty) {
        exprs += F.percentile_approx(
          col(s"`$c`"),
          F.array(numericPercentilePoints.map(F.lit): _*),
          F.lit(10000)
        ).as(s"_pct_$a")
      }
    }
    exprs
  }

  /** Sanitize column names by replacing non-alphanumeric underscore characters with _$idx if a collision occurs */
  private def buildAliases(columns: Seq[String]): Map[String, String] = {
    val seen = scala.collection.mutable.Set[String]()
    columns.map { c =>
      val base  = c.replaceAll("[^a-zA-Z0-9_]", "_")
      var alias = base
      var idx   = 1
      while (seen.contains(alias)) {
        alias = s"${base}_$idx"
        idx  += 1
      }
      seen += alias
      c -> alias
    }.toMap
  }
}
