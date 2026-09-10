package com.pista.spark.sql.conf

/**
 * Metrics collection configuration*
 *
 * Metrics collection switch, output path, queue parameters, and switches and thresholds for each type of metrics.
 */
private[sql] object MetricsConf {

  // ==================== Base Configuration ====================

  val METRICS_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.enabled")
      .doc("Whether metrics collection is enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  val METRICS_OUTPUT_PATH: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("metrics.output.path")
      .doc("output directory")
      .version("1.0.0")
      .stringConf
      .createWithDefault(".")

  val METRICS_QUEUE_CAPACITY: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("metrics.queue.capacity")
      .doc("Queue Capacity")
      .version("1.0.0")
      .intConf
      .createWithDefault(10000)

  val METRICS_QUEUE_DROP_POLICY: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("metrics.queue.dropPolicy")
      .doc("queue full discard policy: oldest or newestoldest or newest")
      .version("1.0.0")
      .stringConf
      .createWithDefault("oldest")

  val METRICS_BATCH_SIZE: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("metrics.batch.size")
      .doc("batch write size")
      .version("1.0.0")
      .intConf
      .createWithDefault(100)

  val METRICS_FLUSH_INTERVAL_MS: ConfigEntryWithDefault[Long] =
    PistaConfigBuilder("metrics.flush.intervalMs")
      .doc("Interval for periodic refresh (milliseconds)")
      .version("1.0.0")
      .longConf
      .createWithDefault(5000L)

  // ==================== Metrics Type Switch ====================

  val METRICS_EXECUTOR_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.executor.enabled")
      .doc("whether enabling Executor resource monitoring Executor resource monitoring")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_SKEW_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.skew.enabled")
      .doc("Whether data skew detection is enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_SHUFFLE_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.shuffle.enabled")
      .doc("Whether enabled Shuffle performance analysis enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_ERROR_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.error.enabled")
      .doc("Whether error diagnosis is enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_PLAN_ENABLED: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("metrics.plan.enabled")
      .doc("Whether query plan analysis is enabled")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  // ==================== Detailed Metrics Configuration ====================

  val METRICS_SKEW_THRESHOLD: ConfigEntryWithDefault[Double] =
    PistaConfigBuilder("metrics.skew.threshold")
      .doc("threshold for skew in data (durationSkewRatio)durationSkewRatio)")
      .version("1.0.0")
      .doubleConf
      .createWithDefault(3.0)

  val METRICS_SKEW_TOPN: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("metrics.skew.topN")
      .doc("sharded partition TopN number")
      .version("1.0.0")
      .intConf
      .createWithDefault(10)

  val METRICS_PLAN_TRUNCATE_LENGTH: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("metrics.plan.truncate.length")
      .doc("Truncate Length of Query Plan")
      .version("1.0.0")
      .intConf
      .createWithDefault(10000)

  val METRICS_DQ_MAX_COLUMNS: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("metrics.dq.maxColumns")
      .doc("maximum number of columns for maximum quality observation statistics (shared between INSERT and SELECT paths, with numerical columns prioritized for retention)")
      .version("2.1.0")
      .intConf
      .createWithDefault(50)

  // ==================== Aggregation ====================

  val entries: Seq[ConfigEntry[_]] = Seq(
    METRICS_ENABLED, METRICS_OUTPUT_PATH, METRICS_QUEUE_CAPACITY,
    METRICS_QUEUE_DROP_POLICY, METRICS_BATCH_SIZE, METRICS_FLUSH_INTERVAL_MS,
    METRICS_EXECUTOR_ENABLED, METRICS_SKEW_ENABLED, METRICS_SHUFFLE_ENABLED, METRICS_ERROR_ENABLED,
    METRICS_PLAN_ENABLED,
    METRICS_SKEW_THRESHOLD, METRICS_SKEW_TOPN, METRICS_PLAN_TRUNCATE_LENGTH,
    METRICS_DQ_MAX_COLUMNS
  )
}
