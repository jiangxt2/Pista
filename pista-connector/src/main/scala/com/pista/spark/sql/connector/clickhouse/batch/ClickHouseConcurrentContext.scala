package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.clickhouse.meta.MetaManager

/**
 * ClickHouse concurrent write context
 *
 * encapsulating Meta service dependencies and parallel scheduling parameters, corresponding to Spark writing pipelines TaskAttemptContext layer.
 * Separate explicitly from pure static configuration (ClickHouseBatchConfig) with clear responsibility boundaries:
 * - ClickHouseBatchConfig: What to write, where to write, and how to write (pure values with no service dependencies)
 * - ClickHouseConcurrentContext: What resources are used for writing and how is concurrency scheduled (including service references)
 *
 * @param metaManager             Meta Manager; required in multi-machine (machineCount > 1), optional in single-machine (None skips checkpointing/progress tracking)
 * @param clusterName             ClickHouse Cluster Name; required for multi-node, optional for single-node; must be specified.
 * @param machineCount            Number of concurrent shards (target ClickHouse nodes)
 * @param maxThreads              Maximum concurrent threads (default 5)
 * @param maxRetries              Maximum shard retry count for failures (default is 3)
 * @param hostAddress             Host address is specified directly when no meta is available (extracted from output.options by ClickHouseWriter)
 * @param prepartitionDir         Prepartition checkpoint directory; set to None to skip pre-partition optimization
 * @param prepartitionFormat      Prepartition file format (parquet/orc, default parquet)
 * @param prepartitionCompression Pre-partition compression algorithm (default: zstd)
 */
case class ClickHouseConcurrentContext(
  metaManager:             Option[MetaManager] = None,
  clusterName:             Option[String]      = None,
  machineCount:            Int,
  maxThreads:              Int           = 5,
  maxRetries:              Int           = 3,
  hostAddress:             Option[String] = None,
  prepartitionDir:         Option[String] = None,
  prepartitionFormat:      String         = "parquet",
  prepartitionCompression: String         = "zstd"
)
