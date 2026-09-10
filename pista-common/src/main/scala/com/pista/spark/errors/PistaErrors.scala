package com.pista.spark.errors

/**
 * Pista generates an error class
 *
 * using a custom exception class, following Sparks design pattern Spark design pattern
 * Error classes are defined in pista-common/error-classes.json, including the imported Spark catalog.
 *
 */
object PistaErrors {

  // ==================== Helper Methods ====================

  private def toSQLConf(conf: String): String = {
    "\"" + conf + "\""
  }

  private def toSQLValue(value: String): String = {
    "\"" + value + "\""
  }

  // ==================== CONFIGURATION ERROR ====================

  def missingRequiredConfigError(configKey: String): PistaConfigException = {
    new PistaConfigException(
      "PISTA_CONFIG_MISSING_REQUIRED",
      Map("configKey" -> toSQLConf(configKey)))
  }

  def invalidConfigValueError(
      configKey: String,
      value: String,
      expected: String): PistaConfigException = {
    new PistaConfigException(
      "PISTA_CONFIG_INVALID_VALUE",
      Map(
        "configKey" -> toSQLConf(configKey),
        "value" -> toSQLValue(value),
        "expected" -> expected))
  }

  // ==================== SQL FILE ERROR ====================

  def invalidSqlFileError(filePath: String): PistaSQLFileException = {
    new PistaSQLFileException(
      "PISTA_INVALID_SQL_FILE",
      Map("filePath" -> filePath))
  }

  def sqlParseError(filePath: String, reason: String): PistaSQLFileException = {
    new PistaSQLFileException(
      "PISTA_SQL_PARSE_ERROR",
      Map(
        "filePath" -> filePath,
        "reason" -> reason))
  }

  def sqlExecutionFailedError(
      failedStatements: Int,
      totalStatements: Int): PistaExecutionException =
    new PistaExecutionException(
      "PISTA_SQL_EXECUTION_FAILED",
      Map(
        "failedStatements" -> failedStatements.toString,
        "totalStatements" -> totalStatements.toString))

  // ==================== Writer Error ====================

  def writerNotFoundError(
      format: String,
      availableWriters: Seq[String]): PistaWriterException = {
    new PistaWriterException(
      "PISTA_WRITER_NOT_FOUND",
      Map(
        "format" -> format,
        "availableWriters" -> availableWriters.mkString(", ")))
  }

  def writerValidationError(reason: String): PistaWriterException = {
    new PistaWriterException(
      "PISTA_WRITER_VALIDATION_FAILED",
      Map("reason" -> reason))
  }

  def unsupportedOutputFormatError(
      format: String,
      supportedFormats: Seq[String]): PistaWriterException = {
    new PistaWriterException(
      "PISTA_UNSUPPORTED_OUTPUT_FORMAT",
      Map(
        "format" -> format,
        "supportedFormats" -> supportedFormats.mkString(", ")))
  }

  // ==================== Reader Error ====================

  def readerNotFoundError(
      format: String,
      availableReaders: Seq[String]): PistaReaderException = {
    new PistaReaderException(
      "PISTA_READER_NOT_FOUND",
      Map(
        "format" -> format,
        "availableReaders" -> availableReaders.mkString(", ")))
  }

  def readerConnectionError(source: String, reason: String): PistaReaderException = {
    new PistaReaderException(
      "PISTA_READER_CONNECTION_FAILED",
      Map(
        "source" -> source,
        "reason" -> reason))
  }

  // ==================== Processor Error ====================

  def processorClassNotFoundError(className: String): PistaProcessorException = {
    new PistaProcessorException(
      "PISTA_PROCESSOR_CLASS_NOT_FOUND",
      Map("className" -> className))
  }

  def processorExecutionError(
      processorName: String,
      reason: String,
      cause: Throwable = null): PistaProcessorException = {
    new PistaProcessorException(
      "PISTA_PROCESSOR_EXECUTION_FAILED",
      Map(
        "processorName" -> processorName,
        "reason" -> reason),
      cause)
  }

  // ==================== Column Type Error ====================

  def invalidParamTypeError(typeName: String): PistaConfigException = {
    new PistaConfigException(
      "PISTA_PARAM_INVALID_TYPE",
      Map("type" -> toSQLValue(typeName)))
  }

  def paramTypeConversionError(
      value: String,
      typeName: String,
      reason: String): PistaConfigException = {
    new PistaConfigException(
      "PISTA_PARAM_TYPE_CONVERSION_FAILED",
      Map(
        "value" -> toSQLValue(value),
        "type" -> toSQLValue(typeName),
        "reason" -> reason))
  }

  // ==================== Checkpoint Error ====================

  def checkpointNotInitializedError(): PistaCheckpointException = {
    new PistaCheckpointException(
      "PISTA_CHECKPOINT_NOT_INITIALIZED",
      Map.empty)
  }

  def checkpointSaveError(path: String, reason: String): PistaCheckpointException = {
    new PistaCheckpointException(
      "PISTA_CHECKPOINT_SAVE_FAILED",
      Map(
        "path" -> path,
        "reason" -> reason))
  }

  def checkpointRestoreError(path: String, reason: String): PistaCheckpointException = {
    new PistaCheckpointException(
      "PISTA_CHECKPOINT_RESTORE_FAILED",
      Map(
        "path" -> path,
        "reason" -> reason))
  }

  // ==================== Data Quality Errors ====================

  def dataQualityValidationError(processorName: String, summary: String): PistaProcessorException =
    new PistaProcessorException(
      "PISTA_PROCESSOR_EXECUTION_FAILED",
      Map("processorName" -> processorName, "reason" -> summary))

  /** Data quality metric collection failure (distinguished from threshold-triggered dataQualityValidationError) */
  def dataQualityExtractError(reason: String, cause: Throwable): PistaProcessorException =
    new PistaProcessorException(
      "PISTA_DATA_QUALITY_EXTRACT_FAILED",
      Map("reason" -> reason),
      cause)

  // ==================== ClickHouse batch write errors ====================

  def clickHouseWriterError(reason: String, cause: Throwable = null): PistaClickHouseException =
    new PistaClickHouseException(
      "PISTA_CLICKHOUSE_WRITER_FAILED",
      Map("reason" -> reason),
      cause)

  def clickHouseEmptyInputError(location: String): PistaClickHouseException =
    new PistaClickHouseException(
      "PISTA_CLICKHOUSE_EMPTY_INPUT",
      Map("location" -> location))

  def clickHouseRecordError(reason: String, cause: Throwable = null): PistaClickHouseException =
    new PistaClickHouseException(
      "PISTA_CLICKHOUSE_RECORD_FAILED",
      Map("reason" -> reason),
      cause)

  // ==================== Doris batch-write errors ====================

  def dorisWriterError(reason: String, cause: Throwable = null): PistaDorisException =
    new PistaDorisException(
      "PISTA_DORIS_WRITER_FAILED",
      Map("reason" -> reason),
      cause)

}
