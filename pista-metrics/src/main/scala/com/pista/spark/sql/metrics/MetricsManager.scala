package com.pista.spark.sql.metrics

import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.metrics.collector.{DataQualityExtractor, InsertMetricsListener, InsertObserveRule, MetricsSparkListener}
import com.pista.spark.sql.metrics.model.Metrics
import com.pista.spark.sql.metrics.writer.{DropPolicy, JsonLineWriter, MetricsQueue, WriterStats}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * MetricsManager
 *
 * Manage lifecycle of metric collection uniformly:
 * - SparkListener.register()
 * - start JsonLineWriter
 * - close resources
 *
 */
class MetricsManager(spark: SparkSession, config: MetricsConfig) extends Logging {
  private val NumericPercentilesEnabled = true
  private val NumericPercentilePoints: Seq[Double] = Seq(0.5, 0.95, 0.99)

  private var queue: MetricsQueue[Metrics] = _
  private var writer: JsonLineWriter = _
  private var listener: MetricsSparkListener = _
  private var insertListener: InsertMetricsListener = _
  private var registered = false
  @volatile private var shutdownHookThread: Thread = _

  /**
   * Register listener and start writer
   */
  def register(): Unit = {
    if (!config.enabled) {
      logInfo("Metrics collection is disabled")
      return
    }

    if (registered) {
      logWarning("MetricsManager already registered")
      return
    }

    // 1. Create queue
    queue = new MetricsQueue[Metrics](config.queueCapacity, config.dropPolicy)

    // 2. Start writer
    writer = new JsonLineWriter(
      outputDir = config.outputDir,
      queue = queue,
      batchSize = config.batchSize,
      flushIntervalMs = config.flushIntervalMs
    )
    writer.start()

    // Register listener
    val appId = spark.sparkContext.applicationId
    val appName = spark.sparkContext.appName
    listener = new MetricsSparkListener(appId, appName, queue, config)
    spark.sparkContext.addSparkListener(listener)

    // 4. Register INSERT Quality Observations
    val extractor = new DataQualityExtractor()
    val rule = new InsertObserveRule(
      maxColumns = config.dqMaxColumns,
      numericPercentilesEnabled = NumericPercentilesEnabled,
      numericPercentilePoints = NumericPercentilePoints
    )
    spark.experimental.extraOptimizations ++= Seq(rule)

    insertListener = new InsertMetricsListener(
      extractor = extractor,
      maxColumns = config.dqMaxColumns,
      numericPercentilesEnabled = NumericPercentilesEnabled,
      numericPercentilePoints = NumericPercentilePoints,
      appId = appId,
      queue = queue
    )
    spark.listenerManager.register(insertListener)

    logInfo(
      s"INSERT data quality observation enabled, maxColumns=${config.dqMaxColumns}, " +
        s"percentilesEnabled=$NumericPercentilesEnabled, " +
        s"percentilePoints=${NumericPercentilePoints.mkString(",")}"
    )

    // 5. Register JVM shutdown hook (registered state decoupled from registration)
    registerShutdownHook()

    registered = true
    logInfo(s"MetricsManager registered, output dir: ${config.outputDir}")
  }

  private def registerShutdownHook(): Unit = synchronized {
    if (shutdownHookThread == null) {
      shutdownHookThread = new Thread(() => close())
      Runtime.getRuntime.addShutdownHook(shutdownHookThread)
    }
  }

  /**
   * shutdown manager
   */
  def close(): Unit = {
    if (!registered) return

    logInfo("Closing MetricsManager...")

    // Remove listener
    if (listener != null) {
      spark.sparkContext.removeSparkListener(listener)
    }

    if (insertListener != null) {
      spark.listenerManager.unregister(insertListener)
    }

    // Close the writer (flushing any remaining data)
    if (writer != null) {
      writer.stop()
    }

    registered = false

    // Unregister Hook to allow for a new Hook to be registered via register() on next invocation
    // JVM shutdown period, Try ignores IllegalStateException thrown by removeShutdownHook.
    synchronized {
      if (shutdownHookThread != null) {
        Try(Runtime.getRuntime.removeShutdownHook(shutdownHookThread))
        shutdownHookThread = null
      }
    }

    logInfo("MetricsManager closed")
  }

  /**
   * Get writer statistics information
   */
  def stats: Option[WriterStats] =
    if (writer != null) Some(writer.stats) else None

  /**
   * Queue metrics into batch (by non-Listener calls such as DataQualityMetricsProcessor)
   */
  def enqueue(metrics: Metrics): Unit =
    if (registered && queue != null) queue.offer(metrics)

  /**
   * Is registered?
   */
  def isRegistered: Boolean = registered
}

/**
 * Metrics configuration*
 */
case class MetricsConfig(
  enabled: Boolean,
  outputDir: String,
  queueCapacity: Int,
  dropPolicy: DropPolicy,
  batchSize: Int,
  flushIntervalMs: Long,
  // Metric type switch
  executorEnabled: Boolean,
  skewEnabled: Boolean,
  skewThreshold: Double,
  skewTopN: Int,
  shuffleEnabled: Boolean,
  errorEnabled: Boolean,
  planEnabled: Boolean,
  planTruncateLength: Int,
  dqMaxColumns: Int
)

object MetricsConfig {
  def fromConfigReader(reader: ConfigReader): MetricsConfig =
    MetricsConfig(
      enabled = reader.get(SubmitterConf.METRICS_ENABLED),
      outputDir = reader.get(SubmitterConf.METRICS_OUTPUT_PATH),
      queueCapacity = reader.get(SubmitterConf.METRICS_QUEUE_CAPACITY),
      dropPolicy = DropPolicy.fromString(reader.get(SubmitterConf.METRICS_QUEUE_DROP_POLICY)),
      batchSize = reader.get(SubmitterConf.METRICS_BATCH_SIZE),
      flushIntervalMs = reader.get(SubmitterConf.METRICS_FLUSH_INTERVAL_MS),
      // Metric type switch
      executorEnabled = reader.get(SubmitterConf.METRICS_EXECUTOR_ENABLED),
      skewEnabled = reader.get(SubmitterConf.METRICS_SKEW_ENABLED),
      skewThreshold = reader.get(SubmitterConf.METRICS_SKEW_THRESHOLD),
      skewTopN = reader.get(SubmitterConf.METRICS_SKEW_TOPN),
      shuffleEnabled = reader.get(SubmitterConf.METRICS_SHUFFLE_ENABLED),
      errorEnabled = reader.get(SubmitterConf.METRICS_ERROR_ENABLED),
      planEnabled = reader.get(SubmitterConf.METRICS_PLAN_ENABLED),
      planTruncateLength = reader.get(SubmitterConf.METRICS_PLAN_TRUNCATE_LENGTH),
      dqMaxColumns = reader.get(SubmitterConf.METRICS_DQ_MAX_COLUMNS)
    )
}

object MetricsManager extends Logging {

  @volatile private var instance: MetricsManager = _

  /**
   * Initialize and register (in SparkSQLSubmitter.main)
   *
   * Idempotent Protection: Return directly if initialized and registered to avoid leaking SparkListener.
   *shutdown()* after can be re-initialized (at this point, *instance.isRegistered* = false).
   */
  def initialize(spark: SparkSession): Unit = synchronized {
    if (instance != null && instance.isRegistered) {
      logWarning("MetricsManager already initialized, skipping")
      return
    }
    val reader = ConfigReader(spark)
    val config = MetricsConfig.fromConfigReader(reader)
    instance = new MetricsManager(spark, config)
    instance.register()
  }

  /**
   * Get instance
   */
  def get: Option[MetricsManager] = Option(instance)

  /**
   * close instance
   */
  def shutdown(): Unit = {
    Option(instance).foreach(_.close())
    instance = null
  }
}
