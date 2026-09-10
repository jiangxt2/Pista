package com.pista.spark.sql.execution.checkpoint

import com.pista.spark.sql.execution.datasources.reader.{AbstractDataReader, InputConfig}
import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import com.pista.spark.util.LogRedaction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Checkpoint Reader
 *
 * Reuse existing Reader logic to read DataFrame from checkpoint directory.
 * Use Parquet format (with CheckpointWriter).
 *
 */
class CheckpointReader extends AbstractDataReader {

  override def name: String = "checkpoint"

  override protected def engineName: String = "CheckpointReader"

  /**
   * validate configuration
   */
  override def validateConfig(config: InputConfig): ValidationResult = {
    if (config.path.isEmpty) {
      ValidationResult(valid = false, Some("Checkpoint path is required"))
    } else {
      ValidationResult(valid = true, None)
    }
  }

  /**
   * internal read method
   */
  override protected def readInternal(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame = {
    val path = config.path.getOrElse(
      throw new IllegalArgumentException("Checkpoint path is required")
    )

    logInfo(s"[$engineName] Reading checkpoint ${LogRedaction.fingerprint(path)}")

    val reader = spark.read

    schema.foreach { s =>
      logInfo(s"[$engineName] Applying explicit schema with ${s.fields.length} fields")
      reader.schema(s)
    }

    config.options.foreach { case (key, value) => reader.option(key, value) }

    val df = reader.format(config.format).load(path)

    if (config.columns.nonEmpty) {
      logInfo(s"[$engineName] Applying column projection: ${config.columns.mkString(", ")}")
      df.select(config.columns.head, config.columns.tail: _*)
    } else {
      df
    }
  }

  /**
   * Predicate Pushdown is supported (native support for Parquet)
   */
  override def supportsPushdown: Boolean = true

  /**
   * Support for partition pruning (native support in Parquet)
   */
  override def supportsPartitionPruning: Boolean = true

  /**
   * Infer Schema (from Parquet File)
   */
  override def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] = {
    config.path.map { path =>
      logInfo(s"[$engineName] Inferring checkpoint schema ${LogRedaction.fingerprint(path)}")
      spark.read.format(config.format).load(path).schema
    }
  }
}
