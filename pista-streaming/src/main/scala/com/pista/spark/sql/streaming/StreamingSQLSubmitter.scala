package com.pista.spark.sql.streaming

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.execution.processor.{ProcessContext, ProcessorManager}
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import com.pista.spark.sql.functions.PistaFunctionInstaller
import com.pista.spark.util.{LogRedaction, SQLTemplateEngine}
import com.pista.spark.sql.streaming.metrics.StreamingMetricsManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DataType

import java.util

/**
 * Streaming SQL Processor
 *
 * Read data from streaming data sources such as Kafka, perform SQL transformations, and write to external storage.
 *
 * Core Process:
 * 1. Create a streaming data source (readStream)
 * 2. Register as a temporary view
 * 3. Execute user SQL (must reference streaming views)
 * 4. Process each micro-batch with foreachBatch
 * 5. Run the processor chain and writer inside foreachBatch
 *
 */
object StreamingSQLSubmitter extends Logging {

  private lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val conf = ConfigReader(sparkSession)

    configureLogLevel(conf)

    // register UDF
    PistaFunctionInstaller.install(sparkSession)

    // Initialize stream metrics collection
    StreamingMetricsManager.initialize(sparkSession)

    // Get SQL file path
    val sqlFilePath = conf.get(SubmitterConf.SQL_FILE).getOrElse {
      throw PistaErrors.missingRequiredConfigError(SubmitterConf.SQL_FILE.key)
    }

    val params = extractParameters()

    submitStreamingSQLFile(sqlFilePath, params)
  }

  private def configureLogLevel(conf: ConfigReader): Unit = {
    val logLevel = conf.get(SubmitterConf.LOG_LEVEL)
    sparkSession.sparkContext.setLogLevel(logLevel)
  }

  private def extractParameters(): util.HashMap[String, String] = {
    val params = new util.HashMap[String, String]()
    val conf = ConfigReader(sparkSession)

    // Extract only spark.pista.params.* configuration.
    for ((paramName, value) <- conf.getAllWithPrefix(SubmitterConf.PARAMS_FULL_PREFIX)) {
      params.put(paramName, value)
    }

    params
  }

  private def submitStreamingSQLFile(sqlFilePath: String, params: util.HashMap[String, String]): Unit = {
    val conf = ConfigReader(sparkSession)

    // 1. Execute initialization SQL (optional)
    conf.get(SubmitterConf.STREAMING_INIT_SQL).foreach { initSql =>
      logInfo(s"Executing init SQL, fingerprint=${LogRedaction.fingerprint(initSql)}")
      sparkSession.sql(initSql)
    }

    // 2. Create a streaming data source
    val streamingSource = createStreamingSource(conf)

    // Register as a temporary view.
    val viewName = conf.get(SubmitterConf.STREAMING_INPUT_VIEW)
    streamingSource.createOrReplaceTempView(viewName)
    logInfo(s"Registered streaming source as view: $viewName")

    // 4. Read and process SQL template
    val sqlContent = SQLTemplateEngine.readSQLFile(sqlFilePath)
    val processedSQL = SQLTemplateEngine.processSQLTemplate(sqlContent, params)

    // 5. Validate that there is only one SQL statement
    val statements = SQLTemplateEngine.splitStatements(processedSQL)
    if (statements.length > 1)
      throw PistaErrors.invalidConfigValueError(
        "sql.statements",
        s"${statements.length} statements",
        "Streaming mode only supports single SQL statement"
      )

    val sqlStatement = statements.head
    logInfo(
      s"Executing streaming SQL, fingerprint=${LogRedaction.fingerprint(sqlStatement)}")

    // 6. Execute SQL to obtain the result DataFrame
    val resultDF = sparkSession.sql(sqlStatement)

    // 7. Validate that the result is a streaming DataFrame
    if (!resultDF.isStreaming)
      throw new IllegalStateException(
        s"SQL result is not a streaming DataFrame. " +
          s"Please ensure your SQL references the streaming view '$viewName'"
      )

    // 8. Start streaming query
    val useSparkSink = conf.get(SubmitterConf.STREAMING_USE_SPARK_SINK)
    if (useSparkSink)
      startWithSparkSink(resultDF, conf)
    else
      startWithForeachBatch(resultDF, conf)
  }

  /**
   * Create a streaming data source
   */
  private def createStreamingSource(conf: ConfigReader): DataFrame = {
    val format = conf.get(SubmitterConf.STREAMING_INPUT_FORMAT)
    val options = conf.getAllWithPrefix(SubmitterConf.STREAMING_INPUT_OPTIONS_PREFIX)

    logInfo(s"Creating streaming source with format: $format")
    logInfo(s"Streaming option keys: ${LogRedaction.optionKeys(options)}")

    val reader = sparkSession.readStream.format(format).options(options)

    // Process Kafka value parsing (optional)
    val valueFormat = conf.get(SubmitterConf.STREAMING_INPUT_VALUE_FORMAT)
    if (valueFormat.isDefined) {
      val schemaStr = conf.get(SubmitterConf.STREAMING_INPUT_VALUE_SCHEMA).getOrElse {
        throw PistaErrors.missingRequiredConfigError(SubmitterConf.STREAMING_INPUT_VALUE_SCHEMA.key)
      }

      val rawDF = reader.load()
      val parsedDF = valueFormat.get.toLowerCase match {
        case "avro" =>
          import org.apache.spark.sql.avro.functions.from_avro
          import org.apache.spark.sql.functions.col
          logInfo(
            s"Parsing Kafka value as Avro, schema fingerprint=" +
              LogRedaction.fingerprint(schemaStr))
          rawDF.select(
            col("key"),
            from_avro(col("value"), schemaStr).as("value"),
            col("topic"), col("partition"), col("offset"),
            col("timestamp"), col("timestampType")
          )

        case other =>
          import org.apache.spark.sql.functions._
          val schema = DataType.fromDDL(schemaStr).asInstanceOf[org.apache.spark.sql.types.StructType]
          logInfo(
            s"Parsing Kafka value as $other, schema fingerprint=" +
              LogRedaction.fingerprint(schemaStr))
          other match {
            case "json" =>
              rawDF.select(
                col("key"),
                from_json(col("value").cast("string"), schema).as("value"),
                col("topic"), col("partition"), col("offset"),
                col("timestamp"), col("timestampType")
              )
            case "csv" =>
              rawDF.select(
                col("key"),
                from_csv(col("value").cast("string"), schema, Map.empty[String, String]).as("value"),
                col("topic"), col("partition"), col("offset"),
                col("timestamp"), col("timestampType")
              )
            case _ =>
              throw PistaErrors.invalidConfigValueError(
                SubmitterConf.STREAMING_INPUT_VALUE_FORMAT.key, other, "json, csv or avro"
              )
          }
      }
      parsedDF
    } else {
      // Parse no value, return original DataFrame directly.
      reader.load()
    }
  }

  /**
   * Start streaming query using foreachBatch mode
   */
  private def startWithForeachBatch(resultDF: DataFrame, conf: ConfigReader): Unit = {
    val (outputConfig, checkpointLocation, queryName) = extractStreamingConfig(conf)

    // Load data processor (only when enabled)
    val processors = ProcessorManager.loadProcessors(sparkSession)
    logInfo(s"Starting streaming query with foreachBatch: $queryName")

    val writer = resultDF.writeStream
      .queryName(queryName)
      .option("checkpointLocation", checkpointLocation)
      .trigger(buildTrigger(conf))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logInfo(s"Processing batch $batchId with ${batchDF.count()} rows")

        var processedDF = batchDF

        // execute Processor pipeline
        if (processors.nonEmpty)
          try {
            val processorContext = ProcessContext(
              sparkSession = sparkSession,
              config = ProcessorManager.extractConfig(sparkSession),
              sqlName = queryName,
              isStreaming = true,
              batchId = Some(batchId),
              queryName = Some(queryName)
            )
            processedDF = ProcessorManager.executeChain(processedDF, processors, processorContext)
          } catch {
            case e: Throwable =>
              logError(
                s"Processor chain failed for batch $batchId with " +
                  LogRedaction.exceptionName(e),
                LogRedaction.sanitizedThrowable(e))
              throw LogRedaction.sanitizedThrowable(e)
          }

        // Write the data to an external storage.
        try
          if (WriterRegistry.isSupported(outputConfig.format)) {
            logInfo(s"Writing batch $batchId using custom writer: ${outputConfig.format}")
            WriterRegistry.write(processedDF, outputConfig)
          } else {
            logInfo(s"Writing batch $batchId using built-in handler: ${outputConfig.format}")
            writeBuiltin(processedDF, outputConfig)
          }
        catch {
          case e: Throwable =>
            logError(
              s"Failed to write batch $batchId with ${LogRedaction.exceptionName(e)}",
              LogRedaction.sanitizedThrowable(e))
            throw LogRedaction.sanitizedThrowable(e)
        }
      }

    val query = writer.start()
    awaitTermination(query, conf)
  }

  /**
   * Launch streaming query using native Spark Sink*
   */
  private def startWithSparkSink(resultDF: DataFrame, conf: ConfigReader): Unit = {
    logWarning("Using Spark native sink mode - Processor chain and custom Writer will be bypassed")

    val (outputConfig, checkpointLocation, queryName) = extractStreamingConfig(conf)

    logInfo(s"Starting streaming query with Spark sink: $queryName")

    var writer = resultDF.writeStream
      .queryName(queryName)
      .option("checkpointLocation", checkpointLocation)
      .trigger(buildTrigger(conf))
      .format(outputConfig.format)
      .outputMode(outputConfig.mode)

    if (outputConfig.path.isDefined)
      writer = writer.option("path", outputConfig.path.get)

    if (outputConfig.options.nonEmpty)
      writer = writer.options(outputConfig.options)

    val query = writer.start()
    awaitTermination(query, conf)
  }

  /**
   * Extract common configurations for streaming queries
   */
  private def extractStreamingConfig(conf: ConfigReader): (OutputConfig, String, String) = {
    val outputConfig = extractOutputConfig(conf)
    val checkpointLocation = conf.get(SubmitterConf.STREAMING_CHECKPOINT_LOCATION).getOrElse {
      throw PistaErrors.missingRequiredConfigError(SubmitterConf.STREAMING_CHECKPOINT_LOCATION.key)
    }
    val queryName = conf.get(SubmitterConf.STREAMING_QUERY_NAME)
      .getOrElse(s"streaming-query-${System.currentTimeMillis()}")

    (outputConfig, checkpointLocation, queryName)
  }

  /**
   * Build Trigger
   */
  private def buildTrigger(conf: ConfigReader): Trigger = {
    val triggerType = conf.get(SubmitterConf.STREAMING_TRIGGER_TYPE)
    val interval = conf.get(SubmitterConf.STREAMING_TRIGGER_INTERVAL)

    triggerType.toLowerCase match {
      case "processing_time" => Trigger.ProcessingTime(interval)
      case "available_now"   => Trigger.AvailableNow()
      case other =>
        throw PistaErrors.invalidConfigValueError(
          SubmitterConf.STREAMING_TRIGGER_TYPE.key,
          other,
          "processing_time, available_now"
        )
    }
  }

  /**
   * wait for stream query termination
   */
  private def awaitTermination(query: StreamingQuery, conf: ConfigReader): Unit = {
    val shouldAwait = conf.get(SubmitterConf.STREAMING_AWAIT_TERMINATION)
    if (shouldAwait) {
      val timeoutMs = conf.get(SubmitterConf.STREAMING_AWAIT_TIMEOUT_MS)
      if (timeoutMs.isDefined) {
        logInfo(s"Waiting for query termination with timeout: ${timeoutMs.get}ms")
        query.awaitTermination(timeoutMs.get.toLong)
      } else {
        logInfo("Waiting for query termination (no timeout)")
        query.awaitTermination()
      }
    } else {
      logInfo("awaitTermination=false, returning immediately")
    }
  }

  /**
   * Extract output configuration
   */
  private def extractOutputConfig(conf: ConfigReader, streamingMode: Boolean = true): OutputConfig = {
    OutputConfig(
      path = conf.get(SubmitterConf.OUTPUT_PATH),
      format = conf.get(SubmitterConf.OUTPUT_FORMAT),
      mode = conf.get(SubmitterConf.OUTPUT_MODE),
      partitionBy = conf.get(SubmitterConf.OUTPUT_PARTITION_BY),
      options = conf.getAllWithPrefix(SubmitterConf.OUTPUT_OPTIONS_PREFIX),
      streamingMode = streamingMode
    )
  }

  /**
   * Write using Spark DataFrameWriter
   */
  private val sparkNativeFormats = Set("parquet", "orc", "json", "csv", "text")

  private def writeBuiltin(result: DataFrame, config: OutputConfig): Unit =
    config.format.toLowerCase match {
      case "console" =>
        val numRows  = config.options.get("numRows").map(_.toInt).getOrElse(20)
        val truncate = config.options.get("truncate").exists(_.toBoolean)
        result.show(numRows, truncate)
      case fmt if sparkNativeFormats.contains(fmt) =>
        if (config.path.isEmpty)
          throw PistaErrors.writerValidationError("Output path is required when using Spark DataFrameWriter")
        var writer = result.write.format(fmt).mode(config.mode)
        if (config.partitionBy.nonEmpty) writer = writer.partitionBy(config.partitionBy: _*)
        if (config.options.nonEmpty)     writer = writer.options(config.options)
        writer.save(config.path.get)
      case _ =>
        throw PistaErrors.writerNotFoundError(config.format, WriterRegistry.availableFormats)
    }
}
