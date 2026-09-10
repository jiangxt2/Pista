package com.pista.spark.sql.connector

/**
 * Batch configuration common trait
 *
 * Common batch-write fields shared by ClickHouseBatchConfig and DorisBatchConfig.
 * Type constrain for general utility methods (JDBC row count query, logging format, etc.).
 */
trait BatchWriteConfig {
  def database: String
  def table: String
  def outputPartitions: Int
  def partitionDate: String
}
