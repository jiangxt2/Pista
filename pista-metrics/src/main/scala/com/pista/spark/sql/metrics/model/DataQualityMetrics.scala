package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Column-level data quality statistics
 *
 * @param nullCount          Number of null values
 * @param nullRatio          Null Ratio
 * @param approxDistinctCount Approximate distinct count (HyperLogLog++, relativeSD=0.05, error ±5%)
 * @param emptyStringCount Count of empty strings (string columns only, equals "")
 * @param blankStringCount Count of blank strings (only for string columns, trimmed to "")
 * @param lengthP50          string length P50(integer or string column)
 * @param lengthP95          string length P95lengthP95 is the 95th percentile of string column lengths.
 * @param minValue           minimum value (applied to numeric columns of consistent original data type)
 * @param maxValue           Maximum value (for numeric columns only, same type as the original column type)
 * @param avgValue           Average value (for numeric columns only)
 * @param stdDevValue       Standard deviation (for numeric columns only)
 * @param p50Value           column P50valueColumn P50 (same type as original column)
 * @param p95Value           column P95$p95Value column represents the 95th percentile value, maintaining the same type as the original column.
 * @param p99Value           column P99$p99Value has a type consistent with the original column.
 *
 */
@JsonInclude(Include.NON_ABSENT)
case class ColumnQualityStats(
  nullCount: Long,
  nullRatio: Double,
  approxDistinctCount: Option[Long] = None,
  emptyStringCount: Option[Long] = None,
  blankStringCount: Option[Long] = None,
  lengthP50: Option[Long] = None,
  lengthP95: Option[Long] = None,
  minValue: Option[Any] = None,
  maxValue: Option[Any] = None,
  avgValue: Option[Double] = None,
  stdDevValue: Option[Double] = None,
  p50Value: Option[Any] = None,
  p95Value: Option[Any] = None,
  p99Value: Option[Any] = None
)

/**
 * data quality metrics
 *
 * @param metricType Metric Type: "data_quality" (SELECT Path) / "insert_data_quality" (INSERT Path)
 * @param appId        Application ID
 * @param executionId SQL Execution ID (unique primary key across sql_metrics.jsonl files for SQL execution)
 * @param tableName    Table name (INSERT path targets the target table; SELECT path identifies by sql_N sequence)
 * @param sourceType Engine type (INSERT path: "v1_hive", "v1_hadoopfs", "v2"; SELECT path: no output)
 * @param sqlIndex     The sequential identifier of the SQL in this execution (e.g., "sql_1")
 * @param totalRecords Total number of records
 * @param columnStats Column statistics per column (null value rate; additional for numeric columns: min/max/avg/stddev)
 * @param collectTimestamp  Collection Timestamp (format: yyyy-MM-dd HH:mm:ss.SSS)
 *
 */
@JsonInclude(Include.NON_ABSENT)
case class DataQualityMetrics(
  metricType: String = "data_quality",
  appId: String,
  executionId: Long,

  // Table information
  tableName: String,
  sourceType: Option[String] = None,
  sqlIndex: Option[String] = None,

  // Basic statistics
  totalRecords: Long,

  // Column-level quality statistics
  columnStats: Map[String, ColumnQualityStats] = Map.empty,

  collectTimestamp: String
) extends Metrics

object DataQualityMetrics {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def toJson(metrics: DataQualityMetrics): String = mapper.writeValueAsString(metrics)
}
