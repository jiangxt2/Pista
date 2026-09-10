package com.pista.spark.sql.conf

import com.pista.spark.errors.PistaErrors

/**
 * Spark SQL Submitter Configuration Aggregator Entry
 *
 * Split all configuration items into independent files by function domain (CoreConf, StreamingConf, etc),
 * This object retains the delegate reference and ensures the external code reference path remains unchanged.
 *
 * Add configuration item checklist:
 * 1. Define ConfigEntry in the corresponding domain file
 * 2. in entries of domain file add
 * 3. Add a delegate reference to this object
 * 4. Add in allEntries
 */
object SubmitterConf {

  // ==================== CoreConf Delegate ====================

  val PREFIX = CoreConf.PREFIX
  val SQL_FILE = CoreConf.SQL_FILE
  val PARAMS_PREFIX = CoreConf.PARAMS_PREFIX
  val PARAMS_FULL_PREFIX = CoreConf.PARAMS_FULL_PREFIX
  val PARAMS_TYPE_SUFFIX = CoreConf.PARAMS_TYPE_SUFFIX
  val PARAMETERIZED_ENABLED = CoreConf.PARAMETERIZED_ENABLED
  val LOG_LEVEL = CoreConf.LOG_LEVEL
  val OUTPUT_PATH = CoreConf.OUTPUT_PATH
  val OUTPUT_FORMAT = CoreConf.OUTPUT_FORMAT
  val OUTPUT_MODE = CoreConf.OUTPUT_MODE
  val OUTPUT_PARTITION_BY = CoreConf.OUTPUT_PARTITION_BY
  val OUTPUT_OPTIONS_PREFIX = CoreConf.OUTPUT_OPTIONS_PREFIX
  val PROCESSOR_ENABLED = CoreConf.PROCESSOR_ENABLED
  val PROCESSOR_CLASSES = CoreConf.PROCESSOR_CLASSES
  val PROCESSOR_PREFIX = CoreConf.PROCESSOR_PREFIX
  val ERROR_POLICY = CoreConf.ERROR_POLICY
  val SQL_ERROR_POLICY = CoreConf.SQL_ERROR_POLICY
  val PROCESSOR_ERROR_POLICY = CoreConf.PROCESSOR_ERROR_POLICY
  val OUTPUT_ERROR_POLICY = CoreConf.OUTPUT_ERROR_POLICY
  val CHECKPOINT_ERROR_POLICY = CoreConf.CHECKPOINT_ERROR_POLICY

  // ==================== StreamingConf Delegate ====================

  val STREAMING_INPUT_FORMAT = StreamingConf.STREAMING_INPUT_FORMAT
  val STREAMING_INPUT_VIEW = StreamingConf.STREAMING_INPUT_VIEW
  val STREAMING_INPUT_VALUE_FORMAT = StreamingConf.STREAMING_INPUT_VALUE_FORMAT
  val STREAMING_INPUT_VALUE_SCHEMA = StreamingConf.STREAMING_INPUT_VALUE_SCHEMA
  val STREAMING_CHECKPOINT_LOCATION = StreamingConf.STREAMING_CHECKPOINT_LOCATION
  val STREAMING_TRIGGER_TYPE = StreamingConf.STREAMING_TRIGGER_TYPE
  val STREAMING_TRIGGER_INTERVAL = StreamingConf.STREAMING_TRIGGER_INTERVAL
  val STREAMING_QUERY_NAME = StreamingConf.STREAMING_QUERY_NAME
  val STREAMING_INIT_SQL = StreamingConf.STREAMING_INIT_SQL
  val STREAMING_USE_SPARK_SINK = StreamingConf.STREAMING_USE_SPARK_SINK
  val STREAMING_AWAIT_TERMINATION = StreamingConf.STREAMING_AWAIT_TERMINATION
  val STREAMING_AWAIT_TIMEOUT_MS = StreamingConf.STREAMING_AWAIT_TIMEOUT_MS
  val STREAMING_INPUT_OPTIONS_PREFIX = StreamingConf.STREAMING_INPUT_OPTIONS_PREFIX

  // ==================== MetricsConf Delegate ====================

  val METRICS_ENABLED = MetricsConf.METRICS_ENABLED
  val METRICS_OUTPUT_PATH = MetricsConf.METRICS_OUTPUT_PATH
  val METRICS_QUEUE_CAPACITY = MetricsConf.METRICS_QUEUE_CAPACITY
  val METRICS_QUEUE_DROP_POLICY = MetricsConf.METRICS_QUEUE_DROP_POLICY
  val METRICS_BATCH_SIZE = MetricsConf.METRICS_BATCH_SIZE
  val METRICS_FLUSH_INTERVAL_MS = MetricsConf.METRICS_FLUSH_INTERVAL_MS
  val METRICS_EXECUTOR_ENABLED = MetricsConf.METRICS_EXECUTOR_ENABLED
  val METRICS_SKEW_ENABLED = MetricsConf.METRICS_SKEW_ENABLED
  val METRICS_SHUFFLE_ENABLED = MetricsConf.METRICS_SHUFFLE_ENABLED
  val METRICS_ERROR_ENABLED = MetricsConf.METRICS_ERROR_ENABLED
  val METRICS_PLAN_ENABLED = MetricsConf.METRICS_PLAN_ENABLED
  val METRICS_SKEW_THRESHOLD = MetricsConf.METRICS_SKEW_THRESHOLD
  val METRICS_SKEW_TOPN = MetricsConf.METRICS_SKEW_TOPN
  val METRICS_PLAN_TRUNCATE_LENGTH = MetricsConf.METRICS_PLAN_TRUNCATE_LENGTH
  val METRICS_DQ_MAX_COLUMNS = MetricsConf.METRICS_DQ_MAX_COLUMNS

  // ==================== MetaConf Delegate ====================

  val META_HOST = MetaConf.META_HOST
  val META_PORT = MetaConf.META_PORT
  val META_DATABASE = MetaConf.META_DATABASE
  val META_USERNAME = MetaConf.META_USERNAME
  val META_PASSWORD = MetaConf.META_PASSWORD
  val META_SCHEMA = MetaConf.META_SCHEMA

  // ==================== ClickHouseConf Delegate ====================

  val CLICKHOUSE_DATABASE = ClickHouseConf.CLICKHOUSE_DATABASE
  val CLICKHOUSE_TABLE = ClickHouseConf.CLICKHOUSE_TABLE
  val CLICKHOUSE_BATCH_SIZE = ClickHouseConf.CLICKHOUSE_BATCH_SIZE
  val CLICKHOUSE_MACHINE_COUNT = ClickHouseConf.CLICKHOUSE_MACHINE_COUNT
  val CLICKHOUSE_CONCURRENT_MAX_THREADS = ClickHouseConf.CLICKHOUSE_CONCURRENT_MAX_THREADS
  val CLICKHOUSE_RETRY_MAX_TIMES = ClickHouseConf.CLICKHOUSE_RETRY_MAX_TIMES
  val CLICKHOUSE_OVERWRITE = ClickHouseConf.CLICKHOUSE_OVERWRITE
  val CLICKHOUSE_OVERWRITE_MODE = ClickHouseConf.CLICKHOUSE_OVERWRITE_MODE
  val CLICKHOUSE_OUTPUT_PARTITIONS = ClickHouseConf.CLICKHOUSE_OUTPUT_PARTITIONS
  val CLICKHOUSE_CLUSTER_NAME = ClickHouseConf.CLICKHOUSE_CLUSTER_NAME
  val CLICKHOUSE_CONVERT_NULL_TO_DEFAULT = ClickHouseConf.CLICKHOUSE_CONVERT_NULL_TO_DEFAULT
  val CLICKHOUSE_PARTITION_DATE = ClickHouseConf.CLICKHOUSE_PARTITION_DATE
  val CLICKHOUSE_PARTITION_COLUMN = ClickHouseConf.CLICKHOUSE_PARTITION_COLUMN
  val CLICKHOUSE_PRIMARY_KEY = ClickHouseConf.CLICKHOUSE_PRIMARY_KEY
  val CLICKHOUSE_PORT = ClickHouseConf.CLICKHOUSE_PORT
  val CLICKHOUSE_HOST = ClickHouseConf.CLICKHOUSE_HOST
  val CLICKHOUSE_PREPARTITION_DIR = ClickHouseConf.CLICKHOUSE_PREPARTITION_DIR
  val CLICKHOUSE_PREPARTITION_FORMAT = ClickHouseConf.CLICKHOUSE_PREPARTITION_FORMAT
  val CLICKHOUSE_PREPARTITION_COMPRESSION = ClickHouseConf.CLICKHOUSE_PREPARTITION_COMPRESSION
  val CLICKHOUSE_CLEAR_DATA_MAX_ATTEMPTS = ClickHouseConf.CLICKHOUSE_CLEAR_DATA_MAX_ATTEMPTS
  val CLICKHOUSE_CLEAR_DATA_INTERVAL_MS = ClickHouseConf.CLICKHOUSE_CLEAR_DATA_INTERVAL_MS
  val CLICKHOUSE_MAX_BACKUP_SWITCHES = ClickHouseConf.CLICKHOUSE_MAX_BACKUP_SWITCHES

  // ==================== DorisConf Delegate ====================

  val DORIS_FENODES = DorisConf.DORIS_FENODES
  val DORIS_DATABASE = DorisConf.DORIS_DATABASE
  val DORIS_TABLE = DorisConf.DORIS_TABLE
  val DORIS_CLUSTER_NAME = DorisConf.DORIS_CLUSTER_NAME
  val DORIS_USER = DorisConf.DORIS_USER
  val DORIS_PASSWORD = DorisConf.DORIS_PASSWORD
  val DORIS_WRITE_MODE = DorisConf.DORIS_WRITE_MODE
  val DORIS_OUTPUT_PARTITIONS = DorisConf.DORIS_OUTPUT_PARTITIONS
  val DORIS_LABEL_PREFIX = DorisConf.DORIS_LABEL_PREFIX
  val DORIS_CONNECTOR_BATCH_SIZE = DorisConf.DORIS_CONNECTOR_BATCH_SIZE
  val DORIS_CONNECTOR_FORMAT = DorisConf.DORIS_CONNECTOR_FORMAT
  val DORIS_CONNECTOR_ENABLE_2PC = DorisConf.DORIS_CONNECTOR_ENABLE_2PC
  val DORIS_AUTO_REDIRECT = DorisConf.DORIS_AUTO_REDIRECT
  val DORIS_BENODES = DorisConf.DORIS_BENODES
  val DORIS_FE_QUERY_PORT = DorisConf.DORIS_FE_QUERY_PORT
  val DORIS_OVERWRITE = DorisConf.DORIS_OVERWRITE
  val DORIS_PARTITION_DATE = DorisConf.DORIS_PARTITION_DATE
  val DORIS_PARTITION_COLUMN = DorisConf.DORIS_PARTITION_COLUMN
  val DORIS_PARTITION_DATE_FORMAT = DorisConf.DORIS_PARTITION_DATE_FORMAT

  // ==================== Aggregation ====================

  val allEntries: Seq[ConfigEntry[_]] =
    CoreConf.entries ++
    StreamingConf.entries ++
    MetricsConf.entries ++
    MetaConf.entries ++
    ClickHouseConf.entries ++
    DorisConf.entries

  // ==================== Cross-Domain Validation ====================

  // No additional required fields format: console, parquet, orc, json, csv, text
  // For additional required fields: clickhouse (host), doris (fenodes)
  // Add Writer when this collection must be updated synchronously
  private val KNOWN_OUTPUT_FORMATS = Set(
    "console", "parquet", "orc", "json", "csv", "text",
    "clickhouse", "doris"
  )

  /**
   * Configure fail-fast during startup.
   *
   * In SparkSQLSubmitter.main() and the public submitSQL() method,
   * Overwrite command line and public API two entry paths.
   */
  def validate(conf: ConfigReader): Unit = {
    val format = conf.get(OUTPUT_FORMAT)

    // Enum value validation
    if (!KNOWN_OUTPUT_FORMATS.contains(format))
      throw PistaErrors.invalidConfigValueError(
        OUTPUT_FORMAT.key, format,
        s"Unknown format, expected one of: ${KNOWN_OUTPUT_FORMATS.mkString(", ")}"
      )

    // Validate mandatory items in ClickHouse
    if (format == "clickhouse" && !conf.contains(CLICKHOUSE_HOST))
      throw PistaErrors.missingRequiredConfigError(CLICKHOUSE_HOST.key)

    // Doris validate mandatory items
    if (format == "doris" && !conf.contains(DORIS_FENODES))
      throw PistaErrors.missingRequiredConfigError(DORIS_FENODES.key)

  }

  /** generate configuration documentation (Markdown format)Markdown format) */
  def generateDoc(): String = {
    val sb = new StringBuilder
    sb.append("# Spark SQL Submitter Configuration Reference\n\n")
    sb.append("| Configuration Item | Type | Default Value | Description |\n")
    sb.append("|--------|------|--------|------|\n")

    for (entry <- allEntries) {
      val typeName = entry match {
        case _: ConfigEntryWithDefault[_] => "Has default value"
        case _: OptionalConfigEntry[_] => "Optional Configuration Entry"
      }
      val defaultVal = entry.defaultValue.map(_.toString).getOrElse("-")
      sb.append(s"| `${entry.key}` | $typeName | $defaultVal | ${entry.doc} |\n")
    }

    sb.toString()
  }
}
