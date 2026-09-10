package com.pista.spark.sql.execution.datasources.reader

import com.pista.spark.sql.execution.datasources.writer.ValidationResult
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * DataReader trait - open for extension via AbstractDataReader
 *
 */
trait DataReader {

  /** Reader Unique Identifier */
  def name: String

  /**
   * Read data from the source.
   * @param spark SparkSession
   * @param config Input configuration
   * @return DataFrame
   */
  def read(spark: SparkSession, config: InputConfig): DataFrame

  /**
   * validate configuration
   * @param config Input configuration
   * @return validation result
   */
  def validateConfig(config: InputConfig): ValidationResult

  /**
   * inference schema SchemaOptional inference schema
   * @param spark SparkSession
   * @param config Input configuration
   * @return inferred Schemareturn None if not supported None
   */
  def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] = None

  /** Support for Predicate Pushdown */
  def supportsPushdown: Boolean = false

  /** Support for sharding pruning */
  def supportsPartitionPruning: Boolean = false
}

/**
 * Abstract base class - direct subclass of DataReader
 * Provides common functionality for all readers
 *
 */
abstract class AbstractDataReader extends DataReader with Logging {

  // ========== Configurable Parameters (Subclasses Can Override) ==========

  /** Engine Name (For Logging) */
  protected def engineName: String = name

  /** Default number of partitions */
  protected def defaultNumPartitions: Int =
    Runtime.getRuntime.availableProcessors() * 2

  // ========== Main Workflow ==========

  override def read(spark: SparkSession, config: InputConfig): DataFrame = {
    // 1. Validate configuration
    val validation = validateConfig(config)
    if (!validation.valid)
      throw new IllegalArgumentException(
        validation.errorMessage.getOrElse("Invalid configuration")
      )

    // 2. Parse Schema (Explicit or Implicit)
    val schema = resolveSchema(spark, config)
    logSchemaInfo(schema)

    // 3. Apply Optimization
    val optimizedConfig = applyOptimizations(config)

    // 4. Read operation
    val startTime = System.currentTimeMillis()
    val df = readInternal(spark, schema, optimizedConfig)
    val readTime = System.currentTimeMillis() - startTime

    // Collect and record statistical information
    val stats = collectReadStats(df, readTime)
    logReadStats(stats)

    df
  }

  /**
   * Internal read method - subclass must implement
   */
  protected def readInternal(
    spark: SparkSession,
    schema: Option[StructType],
    config: InputConfig
  ): DataFrame

  // ========== Schema Processing ==========

  /**
   * Parsing Schemausing explicit schema Schemaotherwise infer
   */
  protected def resolveSchema(
    spark: SparkSession,
    config: InputConfig
  ): Option[StructType] =
    config.schema match {
      case Some(s) =>
        logInfo(s"[$engineName] Using explicit schema with ${s.fields.length} fields")
        Some(s)
      case None if supportsSchemaInference =>
        logInfo(s"[$engineName] Inferring schema from source...")
        inferSchema(spark, config)
      case None =>
        logInfo(s"[$engineName] No schema provided, will use source default")
        None
    }

  /** Support for Schema Inference */
  protected def supportsSchemaInference: Boolean = true

  private def logSchemaInfo(schema: Option[StructType]): Unit =
    schema match {
      case Some(s) =>
        val fields = s.fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(", ")
        logInfo(s"[$engineName] Schema: $fields")
      case None =>
        logInfo(s"[$engineName] Schema will be determined by data source")
    }

  // ========== Optimize ==========

  /**
   * read optimization
   */
  protected def applyOptimizations(config: InputConfig): InputConfig = {
    if (supportsPushdown && config.predicates.nonEmpty)
      logInfo(s"[$engineName] Predicate pushdown enabled: ${config.predicates.mkString(", ")}")

    if (config.columns.nonEmpty)
      logInfo(s"[$engineName] Column projection: ${config.columns.mkString(", ")}")

    if (supportsPartitionPruning && config.partitionFilters.nonEmpty)
      logInfo(s"[$engineName] Applying ${config.partitionFilters.size} partition filter(s)")

    config
  }

  /**
   * Recommend the number of partitions based on estimated rows*
   */
  protected def recommendedPartitions(estimatedRows: Option[Long]): Int =
    estimatedRows match {
      case Some(rows) if rows < 10000 => 1
      case Some(rows) if rows < 100000 => 4
      case Some(rows) if rows < 1000000 => 8
      case Some(rows) if rows < 10000000 => 16
      case _ => defaultNumPartitions
    }

  // ========== Metrics Information ==========

  /**
   * collect read statistics (lightweight, not triggering a full count) count)
   */
  private def collectReadStats(df: DataFrame, readTimeMs: Long): ReadStats =
    ReadStats(
      numPartitions = df.rdd.getNumPartitions,
      numColumns = df.schema.fields.length,
      readTimeMs = readTimeMs,
      estimatedRows = None
    )

  private def logReadStats(stats: ReadStats): Unit =
    logInfo(s"[$engineName] Read stats: ${stats.numPartitions} partitions, " +
      s"${stats.numColumns} columns, ${stats.readTimeMs}ms read time")
}

/**
 **read statistical information (lightweight, not triggering data materialization)**
 *
 * @param numPartitions Number of partitions for the resulting DataFrame
 * @param numColumns Schema number of columns
 * @param readTimeMs The time to create a DataFrame (milliseconds)
 * @param estimatedRows Estimated rows (if available)
 */
case class ReadStats(
  numPartitions: Int,
  numColumns: Int,
  readTimeMs: Long,
  estimatedRows: Option[Long]
)

/**
 * input configuration
 *
 * @param enabled Whether read is enabled
 * @param path DataSource path (table name, file path, etc.)
 * @param format Data format (doris, clickhouse, kafka, etc.)
 * @param schema Explicit Schema (to avoid implicit inference)
 * @param predicates Predicate Pushdown Conditions
 * @param columns Column Selection (read only specified columns)
 * @param partitionFilters Partition filter conditions
 * @param options Format-specific options
 */
case class InputConfig(
  enabled: Boolean,
  path: Option[String],
  format: String,
  schema: Option[StructType] = None,
  predicates: Seq[String] = Seq.empty,
  columns: Seq[String] = Seq.empty,
  partitionFilters: Map[String, String] = Map.empty,
  options: Map[String, String] = Map.empty
)
