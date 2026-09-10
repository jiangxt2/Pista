package com.pista.spark.sql.conf

/**
 * ClickHouse batch write configuration
 *
 * Contains configurations for ClickHouse connection, sharding, retry, overwrite writes, pre-partitioning, and data clearing.
 */
private[sql] object ClickHouseConf {

  val CLICKHOUSE_DATABASE: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.database")
      .doc("ClickHouse Target Database Name")
      .version("2.3")
      .stringConf
      .createOptional

  val CLICKHOUSE_TABLE: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.table")
      .doc("ClickHouse Target Table Name")
      .version("2.3")
      .stringConf
      .createOptional

  val CLICKHOUSE_BATCH_SIZE: ConfigEntryWithDefault[Long] =
    PistaConfigBuilder("clickhouse.batch.size")
      .doc("JDBC batch size, affecting the number of rows per INSERT operation INSERT the number of rows")
      .version("2.3")
      .longConf
      .createWithDefault(200000L)

  val CLICKHOUSE_MACHINE_COUNT: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.machine.count")
      .doc("the number of target write machines, determining the number of shards (must be greater than zero) > 0)")
      .version("2.3")
      .intConf
      .createWithDefault(-1)

  val CLICKHOUSE_CONCURRENT_MAX_THREADS: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.concurrent.maxThreads")
      .doc("maximum number of concurrent writers")
      .version("2.3")
      .intConf
      .createWithDefault(5)

  val CLICKHOUSE_RETRY_MAX_TIMES: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.retry.maxTimes")
      .doc("maximum retry count for failed tasks")
      .version("2.3")
      .intConf
      .createWithDefault(3)

  val CLICKHOUSE_OVERWRITE: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("clickhouse.overwrite")
      .doc("whether overwrites (which resets all task states in metadata and deletes ClickHouse table data) ClickHouse metadata and checkpoint data)")
      .version("2.3")
      .booleanConf
      .createWithDefault(false)

  val CLICKHOUSE_OVERWRITE_MODE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("clickhouse.overwrite.mode")
      .doc("Overwrite cleanup mode: on_cluster (default, uses ON CLUSTER) or per_host (deletes on each host)")
      .version("2.3")
      .stringConf
      .createWithDefault("on_cluster")

  val CLICKHOUSE_OUTPUT_PARTITIONS: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.output.partitions")
      .doc("target partitions during write, controlling JDBC concurrent connections coalesce the number of target partitions, controlling JDBC number of concurrent connections")
      .version("2.3")
      .intConf
      .createWithDefault(10)

  val CLICKHOUSE_CLUSTER_NAME: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.clusterName")
      .doc("ClickHouse Cluster Name")
      .version("2.3")
      .stringConf
      .createOptional

  val CLICKHOUSE_CONVERT_NULL_TO_DEFAULT: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("clickhouse.convertNullToDefault")
      .doc("Convert null values to default values for each type (string->\"\", int/long/float/double->0)")
      .version("2.3")
      .booleanConf
      .createWithDefault(false)

  val CLICKHOUSE_PARTITION_DATE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("clickhouse.partition.dateValue")
      .doc("Partition value used by overwrite=true for DROP PARTITION; empty means TRUNCATE TABLE")
      .version("2.3")
      .stringConf
      .createWithDefault("")

  val CLICKHOUSE_PARTITION_COLUMN: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("clickhouse.partition.columnName")
      .doc("Date partition column (for example, partition_date). Used with partition.dateValue " +
        "to build the post-write row-count predicate; an empty value validates the entire table")
      .version("2.3")
      .stringConf
      .createWithDefault("")

  val CLICKHOUSE_PRIMARY_KEY: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.primaryKey")
      .doc("Primary-key column used for hash-based sharding; required for multi-machine writes")
      .version("2.3")
      .stringConf
      .createOptional

  val CLICKHOUSE_PORT: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.port")
      .doc("ClickHouse HTTP Port for Multi-Node Mode, Used to Construct Write URLs (Default 8123)")
      .version("2.3")
      .intConf
      .createWithDefault(8123)

  val CLICKHOUSE_HOST: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.host")
      .doc("ClickHouse target host used to construct a direct JDBC URL when metadata is disabled; " +
           "falls back to output.options.clickhouse.host")
      .version("2.3")
      .stringConf
      .createOptional

  val CLICKHOUSE_PREPARTITION_DIR: OptionalConfigEntry[String] =
    PistaConfigBuilder("clickhouse.prepartition.dir")
      .doc("Pre-partition materialization directory. Supports hdfs://, s3a://, file://, and paths resolved " +
           "through the default filesystem. Leave unset to disable this optimization.")
      .version("2.5")
      .stringConf
      .createOptional

  val CLICKHOUSE_PREPARTITION_FORMAT: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("clickhouse.prepartition.format")
      .doc("Pre-partition materialization format: parquet (default) or orc")
      .version("2.5")
      .stringConf
      .createWithDefault("parquet")

  val CLICKHOUSE_PREPARTITION_COMPRESSION: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("clickhouse.prepartition.compression")
      .doc("Pre-partition compression (default: zstd). Supported values: zstd, snappy, lz4, and gzip; " +
           "none and uncompressed are rejected.")
      .version("2.5")
      .stringConf
      .createWithDefault("zstd")

  val CLICKHOUSE_CLEAR_DATA_MAX_ATTEMPTS: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.clearData.maxAttempts")
      .doc("Data cleaning validation maximum check count (total timeout = maxAttempts × intervalMs).")
      .version("2.5")
      .intConf
      .createWithDefault(30)

  val CLICKHOUSE_CLEAR_DATA_INTERVAL_MS: ConfigEntryWithDefault[Long] =
    PistaConfigBuilder("clickhouse.clearData.intervalMs")
      .doc("Cleaning validation check interval (milliseconds).")
      .version("2.5")
      .longConf
      .createWithDefault(10000L)

  val CLICKHOUSE_MAX_BACKUP_SWITCHES: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("clickhouse.maxBackupSwitches")
      .doc("Maximum standby switch count (0 = disable switch).")
      .version("2.5")
      .intConf
      .createWithDefault(1)

  val entries: Seq[ConfigEntry[_]] = Seq(
    CLICKHOUSE_DATABASE, CLICKHOUSE_TABLE, CLICKHOUSE_BATCH_SIZE,
    CLICKHOUSE_MACHINE_COUNT, CLICKHOUSE_CONCURRENT_MAX_THREADS, CLICKHOUSE_RETRY_MAX_TIMES,
    CLICKHOUSE_OVERWRITE, CLICKHOUSE_OVERWRITE_MODE, CLICKHOUSE_OUTPUT_PARTITIONS,
    CLICKHOUSE_CLUSTER_NAME,
    CLICKHOUSE_CONVERT_NULL_TO_DEFAULT, CLICKHOUSE_PARTITION_DATE, CLICKHOUSE_PARTITION_COLUMN,
    CLICKHOUSE_PRIMARY_KEY, CLICKHOUSE_PORT, CLICKHOUSE_HOST,
    CLICKHOUSE_PREPARTITION_DIR, CLICKHOUSE_PREPARTITION_FORMAT, CLICKHOUSE_PREPARTITION_COMPRESSION,
    CLICKHOUSE_CLEAR_DATA_MAX_ATTEMPTS, CLICKHOUSE_CLEAR_DATA_INTERVAL_MS, CLICKHOUSE_MAX_BACKUP_SWITCHES
  )
}
