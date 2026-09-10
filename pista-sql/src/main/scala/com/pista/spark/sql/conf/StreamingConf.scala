package com.pista.spark.sql.conf

/**
 * Streaming processing configuration
 *
 * Contains stream processing related configurations such as Kafka input, triggers, Checkpoint, and query names.
 */
private[sql] object StreamingConf {

  val STREAMING_INPUT_OPTIONS_PREFIX =
    s"${PistaConfigBuilder.PREFIX}streaming.input.options."

  val STREAMING_INPUT_FORMAT: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("streaming.input.format")
      .doc("Stream input format: kafka, socket, ratekafka, socket, rate")
      .version("1.1.0")
      .stringConf
      .createWithDefault("kafka")

  val STREAMING_INPUT_VIEW: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("streaming.input.view")
      .doc("name of the streaming input temporary view")
      .version("1.1.0")
      .stringConf
      .createWithDefault("source")

  val STREAMING_INPUT_VALUE_FORMAT: OptionalConfigEntry[String] =
    PistaConfigBuilder("streaming.input.value.format")
      .doc("Kafka value format: json, csv, avro (optional; default is binary if unspecified)json, csv, avrooverwritten by binary data. binary)")
      .version("1.1.0")
      .stringConf
      .createOptional

  val STREAMING_INPUT_VALUE_SCHEMA: OptionalConfigEntry[String] =
    PistaConfigBuilder("streaming.input.value.schema")
      .doc("Kafka value schema: DDL for JSON/CSV or an Avro JSON schema")
      .version("1.1.0")
      .stringConf
      .createOptional

  val STREAMING_CHECKPOINT_LOCATION: OptionalConfigEntry[String] =
    PistaConfigBuilder("streaming.checkpoint.location")
      .doc("Checkpoint directory required for fault-tolerant recovery")
      .version("1.1.0")
      .stringConf
      .createOptional

  val STREAMING_TRIGGER_TYPE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("streaming.trigger.type")
      .doc("Trigger types: processing_time or available_nowprocessing_time, available_now")
      .version("1.1.0")
      .stringConf
      .createWithDefault("processing_time")

  val STREAMING_TRIGGER_INTERVAL: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("streaming.trigger.interval")
      .doc("Trigger interval (such as 10 seconds, 1 minute) 10 seconds, 1 minute)")
      .version("1.1.0")
      .stringConf
      .createWithDefault("10 seconds")

  val STREAMING_QUERY_NAME: OptionalConfigEntry[String] =
    PistaConfigBuilder("streaming.queryName")
      .doc("Stream Query Name (For Monitoring and Logging)")
      .version("1.1.0")
      .stringConf
      .createOptional

  val STREAMING_INIT_SQL: OptionalConfigEntry[String] =
    PistaConfigBuilder("streaming.initSql")
      .doc("Initialization SQL executed before the streaming query starts")
      .version("1.1.0")
      .stringConf
      .createOptional

  val STREAMING_USE_SPARK_SINK: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("streaming.useSparkSink")
      .doc("Use Spark's native sink and bypass the Pista processor and writer pipeline")
      .version("1.1.0")
      .booleanConf
      .createWithDefault(false)

  val STREAMING_AWAIT_TERMINATION: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("streaming.awaitTermination")
      .doc("whether to wait for the termination of streaming queries (false indicates immediate return)false return immediately)")
      .version("1.1.0")
      .booleanConf
      .createWithDefault(true)

  val STREAMING_AWAIT_TIMEOUT_MS: OptionalConfigEntry[Long] =
    PistaConfigBuilder("streaming.awaitTimeoutMs")
      .doc("timeout time (milliseconds, infinite wait if unspecified)")
      .version("1.1.0")
      .longConf
      .createOptional

  val entries: Seq[ConfigEntry[_]] = Seq(
    STREAMING_INPUT_FORMAT, STREAMING_INPUT_VIEW, STREAMING_INPUT_VALUE_FORMAT, STREAMING_INPUT_VALUE_SCHEMA,
    STREAMING_CHECKPOINT_LOCATION, STREAMING_TRIGGER_TYPE, STREAMING_TRIGGER_INTERVAL, STREAMING_QUERY_NAME,
    STREAMING_INIT_SQL, STREAMING_USE_SPARK_SINK, STREAMING_AWAIT_TERMINATION, STREAMING_AWAIT_TIMEOUT_MS
  )
}
