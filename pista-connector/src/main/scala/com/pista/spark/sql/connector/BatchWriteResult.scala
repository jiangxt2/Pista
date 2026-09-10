package com.pista.spark.sql.connector

/** Write Result of a Shard Task */
case class BatchWriteResult(
  index: Int,
  rowCount: Long,
  success: Boolean,
  hostAddress: String = "",
  errorMessage: String = "",
  sourceRowCount: Long = 0L  // Spark source data row count, used to distinguish originalDataVolume and insertDataVolume
)
