package com.pista.spark.sql.connector.clickhouse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * ClickHouse batch write custom UDF
 *
 * Use only in batch write scenarios in ClickHouse, and do not register as a global Expression.
 * When the ClickHouseBatchSubmitter starts, call registerAll.
 *
 * Migrated from legacy implementation functions.scala:
 * - formatArray / formatMap / formatNestedMap: format column / format map / format nested map
 * - convertNull2DefaultValue*:null convert null to default values for various types
 */
object ClickHouseUDFs extends Logging {

  def registerAll(spark: SparkSession): Unit = {
    val udf = spark.udf

    // Temporary compatibility bridge for the legacy ClickHouse shard route.
    // This is intentionally connector-private and must not enter the public
    // Pista function catalog.
    val legacyAbsHashCode: String => java.lang.Integer = value =>
      if (value == null) null else Integer.valueOf(Math.abs(value.hashCode))
    udf.register("ck_legacy_abs_hashcode", legacyAbsHashCode)

    // Array → ClickHouse Array format (separated by \001)
    udf.register("ck_format_array",
      (arr: mutable.WrappedArray[String]) => {
        if (arr == null) ""
        else arr.filter(_ != null).mkString("\001")
      })

    // Map → ClickHouse Map format (key=value,key=value)
    udf.register("ck_format_map",
      (map: Map[String, String]) => {
        if (map == null) ""
        else map.map { case (k, v) => s"$k=${if (v == null) "" else v}" }.mkString(",")
      })

    // Nested Map → ClickHouse Nested Map Format (outerKey\001innerKey\002innerValue\002...)
    udf.register("ck_format_nested_map",
      (map: Map[String, Map[String, String]]) => {
        if (map == null) ""
        else map.map { case (k, v) =>
          val inner = if (v == null) ""
            else v.map { case (ik, iv) => s"$ik\001${if (iv == null) "" else iv}" }.mkString("\002")
          s"$k=$inner"
        }.mkString(",")
      })

    // null → default values for all types
    // Use Java boxed types (rather than Any) to ensure type safety in Spark;
    // Catch malformed values and return the default instead of propagating NumberFormatException.
    udf.register("ck_null_to_default_string",
      (v: java.lang.String) => if (v == null) "" else v)

    udf.register("ck_null_to_default_int",
      (v: java.lang.Integer) => if (v == null) 0 else v.intValue())

    udf.register("ck_null_to_default_long",
      (v: java.lang.Long) => if (v == null) 0L else v.longValue())

    udf.register("ck_null_to_default_float",
      (v: java.lang.Float) => if (v == null) 0.0f else v.floatValue())

    udf.register("ck_null_to_default_double",
      (v: java.lang.Double) => if (v == null) 0.0d else v.doubleValue())

    logInfo("ClickHouse UDFs registered: " +
      "ck_legacy_abs_hashcode, ck_format_array, ck_format_map, ck_format_nested_map, " +
      "ck_null_to_default_string/int/long/float/double")
  }
}
