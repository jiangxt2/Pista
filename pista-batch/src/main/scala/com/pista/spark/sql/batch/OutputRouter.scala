package com.pista.spark.sql.batch

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 **Output Routing: Writer Format Detection (SPI vs. Internal) and Write**
 *
 * Extracted from SparkSQLSubmitter, responsible for a single task.
 */
object OutputRouter extends Logging {

  private val sparkNativeFormats = Set("parquet", "orc", "json", "csv", "text")

  def extractOutputConfig(conf: ConfigReader): OutputConfig = OutputConfig(
    path = conf.get(SubmitterConf.OUTPUT_PATH),
    format = conf.get(SubmitterConf.OUTPUT_FORMAT),
    mode = conf.get(SubmitterConf.OUTPUT_MODE),
    partitionBy = conf.get(SubmitterConf.OUTPUT_PARTITION_BY),
    options = conf.getAllWithPrefix(SubmitterConf.OUTPUT_OPTIONS_PREFIX)
  )

  /**
   * Write results to storage (non-exception swallowing, re-throw original exception)
   *
   * Caller is responsible for the try-catch and actionTriggered logic.
   */
  def write(result: DataFrame, config: OutputConfig): Unit = {
    if (WriterRegistry.isSupported(config.format)) {
      logInfo(s"Writing query result using custom writer: ${config.format}")
      WriterRegistry.write(result, config)
    } else {
      logInfo(s"No custom writer for format ${config.format}, using built-in handler")
      writeBuiltin(result, config)
    }
  }

  private def writeBuiltin(result: DataFrame, config: OutputConfig): Unit =
    config.format.toLowerCase match {
      case "console" =>
        val numRows  = config.options.get("numRows").map(_.toInt).getOrElse(20)
        val truncate = config.options.get("truncate").exists(_.toBoolean)
        result.show(numRows, truncate)
      case fmt if sparkNativeFormats.contains(fmt) =>
        if (config.path.isEmpty)
          throw PistaErrors.writerValidationError("Output path is required when using Spark DataFrameWriter")
        var writer = result.write.format(fmt).mode(config.mode)
        if (config.partitionBy.nonEmpty) writer = writer.partitionBy(config.partitionBy: _*)
        if (config.options.nonEmpty)     writer = writer.options(config.options)
        writer.save(config.path.get)
      case _ =>
        throw PistaErrors.writerNotFoundError(config.format, WriterRegistry.availableFormats)
    }
}
