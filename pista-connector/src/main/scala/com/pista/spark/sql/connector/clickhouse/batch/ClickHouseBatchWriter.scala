package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.clickhouse.{ClickHouseBatchJdbcSupport, ClickHouseDialect, ClickHouseUDFs}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.call_udf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/**
 * ClickHouse Batch Writer Base Class
 *
 * From the BaseBatchWriter in the legacy implementation, migrate the core write process, with the key difference from the original implementation being:
 * - configuration passed through constructor (originally global ExecuteOptions object), supports multi-instance concurrency
 * - No metadata operations; these are orchestrated by the external ClickHouseBatchSubmitter
 * - Return BatchWriteResult instead of the original Int exit code
 *
 * Implement KeyBasedBatchWriter (hash modulo partitioning) for real-time streams or wide table writes.
 *
 * @param config       Batch write configuration
 * @param index        Current shard index (0-based)
 * @param machineCount Total number of shards (target number of machines)
 * @param verbose      whether to emit detailed diagnostic logs
 *                     Avoid log mixing during concurrent tasks with multiple partitions while logging every retry round
 */
abstract class ClickHouseBatchWriter(
  val config: ClickHouseBatchConfig,
  val index: Int,
  val machineCount: Int,
  val verbose: Boolean = true
) extends ClickHouseBatchJdbcSupport with Logging {

  protected implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

  protected val engineName: String = "ClickHouse-Batch"

  /** Read and filter data belonging to this shard */
  protected def loadShardData: DataFrame

  /**
   * Batch write main process*
   *
   * Process: register UDF → load shard data → null transformation → format collection type
   *   → Filter empty rows → Schema Alignment → Write
   */
  def write(): BatchWriteResult = {
    JdbcDialects.registerDialect(ClickHouseDialect)
    ClickHouseUDFs.registerAll(spark)

    logInfo(s"[$engineName] Shard $index/$machineCount starting write")
    logInfo(s"[$engineName] Batch destination configured")
    logInfo(s"[$engineName] Config: skipLocalDelete=${config.skipLocalDelete}, overwrite=${config.overwrite}, partitionDate=${config.partitionDate}")

    // Data cleansing strategy:
    // - Overwrite mode: global delete once (skipLocalDelete=false), then no local delete for any shard (skipLocalDelete=true)
    // - Overwrite mode: Skipped successful partitions (not executed by write) and deleted data from failed partitions (skipLocalDelete=false)
    if (!config.skipLocalDelete) {
      clearPartitionViaJdbc(config.jdbcUrl, config.table, config.partitionDate, config.options)
    }

    var df = loadShardData

    if (config.convertNullToDefault) df = convertNullToDefaults(df)
    df = convertCollectionTypes(df)
    df = alignToSchema(df)

    df.persist(StorageLevel.DISK_ONLY)
    df.rdd.setName(s"clickhouse_shard[$index/$machineCount][${config.table}]")
    val sourceRowCount = df.count()

    writeDataViaJdbc(df, config.jdbcUrl, config.table,
      config.batchSize, config.outputPartitions, config.options)

    df.unpersist(blocking = false)

    // Use partition column + date value to construct the WHERE condition to avoid inflated validation values due to full-table COUNT.
    val countCondition =
      if (config.partitionColumn.nonEmpty && config.partitionDate.nonEmpty)
        s"${config.partitionColumn} = '${config.partitionDate}'"
      else ""
    val rowCount = countRowsViaJdbc(config.jdbcUrl, config.table, config.options, countCondition)

    // aligning the original verifyData() logic——source rows match 0 which requires ck also match 0otherwise, it requires an exact match.
    val success =
      if (sourceRowCount == 0L) rowCount == 0L
      else sourceRowCount == rowCount

    if (!success)
      logWarning(s"[$engineName] Shard $index/$machineCount: data volume mismatch " +
        s"(source=$sourceRowCount, ck-actual=$rowCount, table=${config.table})")
    else
      logInfo(s"[$engineName] Shard $index/$machineCount: source=$sourceRowCount, ck-actual=$rowCount rows to ${config.table}")

    BatchWriteResult(index, rowCount, success, sourceRowCount = sourceRowCount)
  }

  // ==================== Private Process Method ====================

  /** null → Default Values (corresponding to ck_null_to_default_* UDF) */
  private def convertNullToDefaults(df: DataFrame): DataFrame =
    df.schema.foldLeft(df) { (d, f) => f.dataType match {
      case StringType  => d.withColumn(f.name, call_udf("ck_null_to_default_string", d(f.name)))
      case IntegerType => d.withColumn(f.name, call_udf("ck_null_to_default_int", d(f.name)))
      case LongType    => d.withColumn(f.name, call_udf("ck_null_to_default_long", d(f.name)))
      case FloatType   => d.withColumn(f.name, call_udf("ck_null_to_default_float", d(f.name)))
      case DoubleType  => d.withColumn(f.name, call_udf("ck_null_to_default_double", d(f.name)))
      case _ => d
    }}

  /** Array/Map → ClickHouse String Format (corresponding to ck_format_* UDF) */
  private def convertCollectionTypes(df: DataFrame): DataFrame =
    df.schema.foldLeft(df) { (d, f) => f.dataType match {
      case ArrayType(StringType, _) =>
        d.withColumn(f.name, call_udf("ck_format_array", d(f.name)))
      case MapType(StringType, StringType, _) =>
        d.withColumn(f.name, call_udf("ck_format_map", d(f.name)))
      case MapType(StringType, MapType(StringType, StringType, _), _) =>
        d.withColumn(f.name, call_udf("ck_format_nested_map", d(f.name)))
      case _ => d
    }}

  /**
   * Aligns the DataFrame with the ClickHouse table by dropping source columns that are absent from the target schema.
   *
   * This is a critical step in ClickHouse batch write -- ClickHouse does not allow writing extra columns.
   */
  private def alignToSchema(df: DataFrame): DataFrame = {
    val ckCols = getClickHouseTableSchema(config.jdbcUrl, config.table, config.options).names.toSet
    df.schema.names.foldLeft(df) { (d, c) => if (ckCols.contains(c)) d else d.drop(c) }
  }
}
