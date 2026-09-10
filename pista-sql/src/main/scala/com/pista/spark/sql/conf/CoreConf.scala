package com.pista.spark.sql.conf

/**
 * Core Configuration: Base Parameters + Processor + Error Strategy
 *
 * Base Run Configuration with SparkSQLSubmitter:
 * - Global prefix, SQL file path, parameterized query, log level
 * - output format/path/configuration/shards
 * - processor enabled/class name
 * - Global error strategy and coverage strategies at each stage
 */
private[sql] object CoreConf {

  // ==================== Base Configuration ====================

  /** Pista configuration prefix. */
  val PREFIX = PistaConfigBuilder.PREFIX

  val SQL_FILE: OptionalConfigEntry[String] = PistaConfigBuilder("sqlFile_")
    .doc("SQL file path, supporting local paths and HDFS path")
    .version("1.0.0")
    .stringConf
    .createOptional

  // ==================== Parameterized Query Configuration ====================

  /** Relative prefix for extracting Pista SQL parameters. */
  val PARAMS_PREFIX = "params."
  val PARAMS_FULL_PREFIX = s"$PREFIX$PARAMS_PREFIX"

  /** suffix for parameter type declarations */
  val PARAMS_TYPE_SUFFIX = ".type"

  val PARAMETERIZED_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("parameterized.enabled")
      .doc("whether parameterized queries are enabled (using the Spark native SQL API with Sparks sqlText and args) Spark native sql(sqlText, args) API)")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(true)

  val LOG_LEVEL: ConfigEntryWithDefault[String] = PistaConfigBuilder("log.level")
    .doc("Logging levels: DEBUG, INFO, WARN, ERRORDEBUG, INFO, WARN, ERROR")
    .version("1.0.0")
    .stringConf
    .createWithDefault("INFO")

  // ==================== OUTPUT CONFIGURATION ====================

  val OUTPUT_PATH: OptionalConfigEntry[String] =
    PistaConfigBuilder("output.path")
      .doc("output path, supporting HDFS,S3local paths or database table names")
      .version("1.0.0")
      .stringConf
      .createOptional

  val OUTPUT_FORMAT: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("output.format")
      .doc("Output formats: console (default), parquet, orc, json, csv, text, doris, clickhouseconsole(Default), parquet, orc, json, csv, text, doris, clickhouse")
      .version("1.0.0")
      .stringConf
      .createWithDefault("console")

  val OUTPUT_MODE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("output.mode")
      .doc("Write mode: overwrite, append, errorifexists, ignoreoverwrite, append, errorifexists, ignore")
      .version("1.0.0")
      .stringConf
      .createWithDefault("overwrite")

  val OUTPUT_PARTITION_BY: ConfigEntryWithDefault[Seq[String]] =
    PistaConfigBuilder("output.partitionBy")
      .doc("Partitioning field, multiple fields separated by commas.")
      .version("1.0.0")
      .stringSeqConf
      .createWithDefault(Seq.empty)

  /** Prefix for format-specific output options. */
  val OUTPUT_OPTIONS_PREFIX = s"${PREFIX}output.options."

  // ==================== Processor Configuration ====================

  val PROCESSOR_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("processor.enabled")
      .doc("Whether data processing is enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  val PROCESSOR_CLASSES: ConfigEntryWithDefault[Seq[String]] =
    PistaConfigBuilder("processor.classes")
      .doc("List of processor class names, separated by commas")
      .version("1.0.0")
      .stringSeqConf
      .createWithDefault(Seq.empty)

  /** Processor configuration prefix. */
  val PROCESSOR_PREFIX = s"${PREFIX}processor."

  // ==================== Error Handling Configuration ====================

  val ERROR_POLICY: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("errorPolicy")
      .doc("Global error strategy: fail-fast or continue-on-error")
      .version("1.1.0")
      .stringConf
      .createWithDefault("fail-fast")

  val SQL_ERROR_POLICY: OptionalConfigEntry[String] =
    PistaConfigBuilder("sql.errorPolicy")
      .doc("SQL Execution Error Policy (Overwrite Global Policy)")
      .version("1.1.0")
      .stringConf
      .createOptional

  val PROCESSOR_ERROR_POLICY: OptionalConfigEntry[String] =
    PistaConfigBuilder("processor.errorPolicy")
      .doc("Processor Overwrite error strategy with the processors global strategy")
      .version("1.1.0")
      .stringConf
      .createOptional

  val OUTPUT_ERROR_POLICY: OptionalConfigEntry[String] =
    PistaConfigBuilder("output.errorPolicy")
      .doc("Output error strategy overrides global strategy")
      .version("1.1.0")
      .stringConf
      .createOptional

  val CHECKPOINT_ERROR_POLICY: OptionalConfigEntry[String] =
    PistaConfigBuilder("checkpoint.errorPolicy")
      .doc("Checkpoint Overwrite Strategy (Overrides Global Strategy)")
      .version("1.1.0")
      .stringConf
      .createOptional

  // ==================== Aggregation ====================

  val entries: Seq[ConfigEntry[_]] = Seq(
    SQL_FILE, LOG_LEVEL, PARAMETERIZED_ENABLED,
    OUTPUT_PATH, OUTPUT_FORMAT, OUTPUT_MODE, OUTPUT_PARTITION_BY,
    PROCESSOR_ENABLED, PROCESSOR_CLASSES,
    ERROR_POLICY, SQL_ERROR_POLICY, PROCESSOR_ERROR_POLICY, OUTPUT_ERROR_POLICY, CHECKPOINT_ERROR_POLICY
  )
}
