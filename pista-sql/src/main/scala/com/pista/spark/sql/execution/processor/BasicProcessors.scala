package com.pista.spark.sql.execution.processor

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Base data processor collection
 *
 * **Common Data Processor:**
 * - DataMaskingProcessor: Data Masking
 * - TimeDimensionProcessor: Time Dimension Extension
 * - UserTagProcessor: user tag generation
 *
 */

// ============================================================================
// Data anonymization processor
// ============================================================================

/**
 * Data anonymization processor
 *
 * Overwrite sensitive data to protect user privacy.
 *
 * Configuration parameters:
 * - processor.mask.columns: need masked columns configured, format is "column:type,column:type"
 *   Supported Types: phone (Phone Number), email (Email), name (Name), idcard (ID Card)
 *   Example: phone:phone, email:email, name:name
 *
 * De-identification rule:
 * - phone: 138****1678 (keeping the first 3 characters and the last 4 characters)
 * - email: abc***@example.com(@preserve the first3position)
 * - name: Zhang (Preserved Surname)
 * - idcard: 110101********1234the first 6 columns and the last 4 columns6and the end4column retains the first 6 characters and the last 4 characters.
 */
class DataMaskingProcessor extends DataProcessor with Logging {

  override def name: String = "DataMaskingProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    val maskColumnsConfig = context.config.get("mask.columns")

    if (maskColumnsConfig.isEmpty) {
      logWarning("No mask.columns configured, returning original DataFrame")
      df
    } else {
      // Parse configuration: phone:phone, email:email, name:name
      val maskRules = maskColumnsConfig.get.split(",").map { rule =>
        val parts = rule.trim.split(":")
        if (parts.length == 2) {
          Some((parts(0).trim, parts(1).trim))
        } else {
          logWarning(s"Invalid mask rule: $rule")
          None
        }
      }.flatten.toMap

      logInfo(s"Masking columns: ${maskRules.keys.mkString(", ")}")

      var result = df

      // Apply corresponding anonymization rules to each column requiring anonymization.
      maskRules.foreach { case (column, maskType) =>
        if (df.columns.contains(column)) {
          maskType.toLowerCase match {
            case "phone" =>
              result = result.withColumn(column, maskPhone(col(column)))
              logInfo(s"Applied phone masking to column: $column")

            case "email" =>
              result = result.withColumn(column, maskEmail(col(column)))
              logInfo(s"Applied email masking to column: $column")

            case "name" =>
              result = result.withColumn(column, maskName(col(column)))
              logInfo(s"Applied name masking to column: $column")

            case "idcard" =>
              result = result.withColumn(column, maskIdCard(col(column)))
              logInfo(s"Applied idcard masking to column: $column")

            case _ =>
              logWarning(s"Unknown mask type: $maskType for column: $column")
          }
        } else {
          logWarning(s"Column not found: $column")
        }
      }

      result
    }
  }

  /** Phone number anonymization: 138****5678 */
  private def maskPhone(col: org.apache.spark.sql.Column) =
    when(col.isNotNull && length(col) >= 11,
      concat(
        substring(col, 1, 3),
        lit("****"),
        substring(col, 8, 4)
      )
    ).otherwise(col)

  /** Email anonymization: abc\*\*@example.com */
  private def maskEmail(col: org.apache.spark.sql.Column) =
    when(col.isNotNull,
      regexp_replace(col, "^(.{3})[^@]*(@.*)$", "$1***$2")
    ).otherwise(col)

  /** Name anonymization: Zhang** */
  private def maskName(col: org.apache.spark.sql.Column) =
    when(col.isNotNull && length(col) >= 2,
      concat(
        substring(col, 1, 1),
        lit("**")
      )
    ).otherwise(col)

  /** ID Masking: 110101****1234 */
  private def maskIdCard(col: org.apache.spark.sql.Column) =
    when(col.isNotNull && length(col) >= 18,
      concat(
        substring(col, 1, 6),
        lit("********"),
        substring(col, 15, 4)
      )
    ).otherwise(col)
}

// ============================================================================
// Time dimension expansion processor
// ============================================================================

/**
 * time dimension expansion processor
 *
 * Generate time dimension column automatically based on timestamp field for easier subsequent analysis.
 *
 * Configuration parameters:
 * - processor.time.column: TimeColumnName (default: auto-detect timestamp type column)
 * - processor.time.dimensions: need generated dimensions, comma-separated (default: year,month,day,hour,weekday,quarter)
 *
 * Time dimension generated:
 * - year: year (2026)
 * - month: month (1-12)
 * - day: day (1-31)
 * - hour: hours (0-23)
 * - weekday: weekday (1=Monday, 7=Sunday)
 * - quarter: quarter (1-4)
 * - date: date string (2026-01-29)
 */
class TimeDimensionProcessor extends DataProcessor with Logging {

  override def name: String = "TimeDimensionProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    val timeColumnConfig = context.config.get("time.column")
    val dimensionsConfig = context.config.getOrElse("time.dimensions", "year,month,day,hour,weekday,quarter,date")

    // Determine the timestamp column
    val maybeColumn = timeColumnConfig match {
      case Some(col) => Some(col)
      case None =>
        // Detect automatically the column of type timestamp
        val timestampCols = df.schema.fields
          .filter(f => f.dataType.typeName == "timestamp")
          .map(_.name)

        if (timestampCols.isEmpty) {
          logWarning("No timestamp column found, returning original DataFrame")
          None
        } else {
          Some(timestampCols.head)
        }
    }

    maybeColumn match {
      case None => df
      case Some(timeColumn) =>
        if (!df.columns.contains(timeColumn)) {
          logWarning(s"Time column not found: $timeColumn, returning original DataFrame")
          df
        } else {
          logInfo(s"Extracting time dimensions from column: $timeColumn")

          val dimensions = dimensionsConfig.split(",").map(_.trim).toSet
          var result = df

          // Generate metrics for each time dimension
          if (dimensions.contains("year")) {
            result = result.withColumn("year", year(col(timeColumn)))
            logInfo("Added dimension: year")
          }

          if (dimensions.contains("month")) {
            result = result.withColumn("month", month(col(timeColumn)))
            logInfo("Added dimension: month")
          }

          if (dimensions.contains("day")) {
            result = result.withColumn("day", dayofmonth(col(timeColumn)))
            logInfo("Added dimension: day")
          }

          if (dimensions.contains("hour")) {
            result = result.withColumn("hour", hour(col(timeColumn)))
            logInfo("Added dimension: hour")
          }

          if (dimensions.contains("weekday")) {
            result = result.withColumn("weekday", dayofweek(col(timeColumn)))
            logInfo("Added dimension: weekday")
          }

          if (dimensions.contains("quarter")) {
            result = result.withColumn("quarter", quarter(col(timeColumn)))
            logInfo("Added dimension: quarter")
          }

          if (dimensions.contains("date")) {
            result = result.withColumn("date", date_format(col(timeColumn), "yyyy-MM-dd"))
            logInfo("Added dimension: date")
          }

          logInfo(s"Time dimension extraction completed, added ${dimensions.size} dimensions")

          result
        }
    }
  }
}

// ============================================================================
// User label generator processor
// ============================================================================

/**
 * User label generator processor
 *
 * Generate generic user tags from spending, income, and activity inputs.
 *
 * Configuration parameters:
 * - processor.tag.spending.column: spending column name (default: spending)
 * - processor.tag.income.column: income column name (default: income)
 * - processor.tag.active.column: active-days column name (default: active_days)
 *
 * Generated labels:
 * - user_level: low_spending, moderate_spending, high_spending, or very_high_spending
 * - activity_level: low_activity, moderate_activity, or high_activity
 * - spending_power: no_income, frugal, moderate, or high
 * - user_segment: high_value_user, growth_user, churn_risk, or standard_user
 */
class UserTagProcessor extends DataProcessor with Logging {

  override def name: String = "UserTagProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    val spendingCol = context.config.getOrElse("tag.spending.column", "spending")
    val incomeCol = context.config.getOrElse("tag.income.column", "income")
    val activeCol = context.config.getOrElse("tag.active.column", "active_days")

    logInfo("Generating user tags from configured columns")

    // Check for the existence of required columns
    val missingCols = Seq(spendingCol, incomeCol, activeCol).filterNot(df.columns.contains)
    if (missingCols.nonEmpty) {
      logWarning(s"Missing columns: ${missingCols.mkString(", ")}, returning original DataFrame")
      df
    } else {
      var result = df

      // User tier labels based on spending amount.
      result = result.withColumn("user_level",
        when(col(spendingCol) < 5000, "low_spending")
          .when(col(spendingCol) < 15000, "moderate_spending")
          .when(col(spendingCol) < 25000, "high_spending")
          .otherwise("very_high_spending")
      )
      logInfo("Generated tag: user_level")

      // Activity labels based on active days.
      result = result.withColumn("activity_level",
        when(col(activeCol) < 150, "low_activity")
          .when(col(activeCol) < 300, "moderate_activity")
          .otherwise("high_activity")
      )
      logInfo("Generated tag: activity_level")

      // Avoid division by zero and represent missing or non-positive income explicitly.
      result = result.withColumn("spending_power",
        when(col(incomeCol).isNull || col(incomeCol).cast("double") <= 0.0, "no_income")
          .when(col(spendingCol).cast("double") / col(incomeCol).cast("double") < 0.1, "frugal")
          .when(col(spendingCol).cast("double") / col(incomeCol).cast("double") < 0.18, "moderate")
          .otherwise("high")
      )
      logInfo("Generated tag: spending_power")

      // Combine the tier and activity labels into a generic segment.
      result = result.withColumn("user_segment",
        when(col("user_level") === "very_high_spending" && col("activity_level") === "high_activity", "high_value_user")
          .when(col("user_level") === "high_spending" && col("activity_level") === "high_activity", "high_value_user")
          .when(col("user_level") === "moderate_spending" && col("activity_level") === "high_activity", "growth_user")
          .when(col("user_level") === "high_spending" && col("activity_level") === "moderate_activity", "growth_user")
          .when(col("activity_level") === "low_activity", "churn_risk")
          .otherwise("standard_user")
      )
      logInfo("Generated tag: user_segment")

      logInfo("User tag generation completed")

      result
    }
  }
}
