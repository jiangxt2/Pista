package com.pista.spark.sql.execution.processor

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * data processor interface
 *
 * Users can implement this interface to define custom data processing logic.
 * processor is invoked before the output of the query execution. SQL The processor is invoked after the query execution but before the output of the results.
 *
 * Use case:
 * - SQL cannot express complex business logic
 * - Call external API for data augmentation
 * - Complex data anonymization or encryption
 * - inference of model inference
 * - External system complex interactions
 *
 */
trait DataProcessor {

  /**
   * Processor Name
   * For logging and error tracking
   */
  def name: String

  /**
   * Process data
   *
   * @param df Input DataFrame (from SQL query result)
   * @param context Processing context containing information such as SparkSession and configurations.
   * @return Processed DataFrame
   */
  def process(df: DataFrame, context: ProcessContext): DataFrame
}

/**
 * process context
 *
 * Contains context information required for processor execution*
 *
 * @param sparkSession Spark Session, used to create temporary views and execute SQL, etc.
 * @param config Processor configuration, extracted from spark.pista.processor.*
 * @param sqlName The current executed SQL statement (used for logging)
 * @param isStreaming Whether streaming processing mode
 * @param batchId Batch ID for batch processing (valid only in streaming mode)
 * @param queryName Stream query name (valid only in streaming mode)
 * @param postActionCallbacks callbacks registered by a Processor during process() and invoked after the Action completes
 *                            SparkSQLSubmitter triggers sequentially after write/show.
 *                            processorErrorPolicy writes exceptionStrategy
 */
case class ProcessContext(
  sparkSession: SparkSession,
  config: Map[String, String],
  sqlName: String,
  isStreaming: Boolean = false,
  batchId: Option[Long] = None,
  queryName: Option[String] = None,
  postActionCallbacks: mutable.Buffer[() => Unit] = mutable.ArrayBuffer.empty
)
