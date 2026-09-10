package com.pista.spark.sql.execution.checkpoint

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.execution.datasources.writer.{AbstractDataWriter, DataStats, OutputConfig, ValidationResult}
import com.pista.spark.util.LogRedaction
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Checkpoint Writer
 *
 * Writes a DataFrame to the configured materialization directory.
 * Use Parquet format (column-oriented storage, high compression ratio, fast reading).
 *
 */
class CheckpointWriter extends AbstractDataWriter {

  override def name: String = "checkpoint"

  override protected def engineName: String = "CheckpointWriter"

  /**
   * Overwrite parent write process, skip ensureCached + collectDataStats (count).
   * The responsibility of Checkpoint is to materialize DataFrame to disk, where writing parquet is the only necessary Action.
   * No additional persist and row counting is required.
   */
  override def write(df: DataFrame, config: OutputConfig): Unit = {
    val validation = validateConfig(config)
    if (!validation.valid)
      throw PistaErrors.writerValidationError(
        validation.errorMessage.getOrElse("Invalid checkpoint configuration"))
    writeInternal(df, DataStats.empty, config)
  }

  /**
   * validate configuration
   */
  override def validateConfig(config: OutputConfig): ValidationResult = {
    if (config.path.isEmpty) {
      ValidationResult(valid = false, Some("Checkpoint path is required"))
    } else {
      ValidationResult(valid = true, None)
    }
  }

  /**
   * internal write method
   */
  override protected def writeInternal(
    df: DataFrame,
    stats: DataStats,
    config: OutputConfig
  ): Unit = {
    val path = config.path.getOrElse(
      throw new IllegalArgumentException("Checkpoint path is required")
    )

    logInfo(s"[$engineName] Writing checkpoint ${LogRedaction.fingerprint(path)}")
    logInfo(s"[$engineName] Format: ${config.format}, Mode: ${config.mode}")

    val writer = df.write.mode(SaveMode.valueOf(config.mode))
    config.options.foreach { case (key, value) => writer.option(key, value) }
    writer.format(config.format).save(path)

    logInfo(s"[$engineName] Checkpoint written successfully")
  }
}
