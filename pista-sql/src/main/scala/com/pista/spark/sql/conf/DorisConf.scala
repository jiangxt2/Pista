package com.pista.spark.sql.conf

/**
 * Doris batch write configuration
 *
 * Contains configurations for Doris FE/BE connection, write mode, partitions, and overwrite.
 */
private[sql] object DorisConf {

  val DORIS_FENODES: OptionalConfigEntry[String] =
    PistaConfigBuilder("doris.fenodes")
      .doc("Doris FE node addresses, comma-separated (such as ) fe1:8030,fe2:8030)")
      .version("2.4")
      .stringConf
      .createOptional

  val DORIS_DATABASE: OptionalConfigEntry[String] =
    PistaConfigBuilder("doris.database")
      .doc("Doris Target Database Name")
      .version("2.4")
      .stringConf
      .createOptional

  val DORIS_TABLE: OptionalConfigEntry[String] =
    PistaConfigBuilder("doris.table")
      .doc("Doris Target Table Name")
      .version("2.4")
      .stringConf
      .createOptional

  val DORIS_CLUSTER_NAME: OptionalConfigEntry[String] =
    PistaConfigBuilder("doris.clusterName")
      .doc("Doris Cluster Name")
      .version("2.4")
      .stringConf
      .createOptional

  val DORIS_USER: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.user")
      .doc("Doris Authentication Username")
      .version("2.4")
      .stringConf
      .createWithDefault("root")

  val DORIS_PASSWORD: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.password")
      .doc("Doris Authentication Password")
      .version("2.4")
      .stringConf
      .createWithDefault("")

  val DORIS_WRITE_MODE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.writeMode")
      .doc("Write mode: spark_connector (Connector DataFrame API) or stream_load (HTTP client)")
      .version("2.4")
      .stringConf
      .createWithDefault("spark_connector")

  val DORIS_OUTPUT_PARTITIONS: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("doris.outputPartitions")
      .doc("Repartition before writing; the recommended upper bound is four times the BE node count")
      .version("2.4")
      .intConf
      .createWithDefault(50)

  val DORIS_LABEL_PREFIX: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.labelPrefix")
      .doc("Stream Load Label prefix for idempotent marker")
      .version("2.4")
      .stringConf
      .createWithDefault("pista")

  val DORIS_CONNECTOR_BATCH_SIZE: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("doris.connector.batchSize")
      .doc("Connector internal batch Stream Load number of rows")
      .version("2.4")
      .intConf
      .createWithDefault(500000)

  val DORIS_CONNECTOR_FORMAT: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.connector.format")
      .doc("Write format: CSV (default) or JSON; Arrow is disabled because of connector shading conflicts")
      .version("2.4")
      .stringConf
      .createWithDefault("csv")

  val DORIS_CONNECTOR_ENABLE_2PC: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("doris.connector.enable2PC")
      .doc("Is Exactly-Once 2PC transaction enabled (transaction_timeout_second must be increased in Doris FE when enabled)")
      .version("2.4")
      .booleanConf
      .createWithDefault(false)

  val DORIS_AUTO_REDIRECT: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("doris.connector.autoRedirect")
      .doc("true routes through FE using fenodes; false connects directly to BE and requires benodes. " +
        "Set false when BE private addresses are unreachable, such as behind Docker or NAT.")
      .version("2.4")
      .booleanConf
      .createWithDefault(true)

  val DORIS_BENODES: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.connector.benodes")
      .doc("Comma-separated BE HTTP nodes, for example be1:8040,be2:8040; used when autoRedirect=false")
      .version("2.4")
      .stringConf
      .createWithDefault("")

  val DORIS_FE_QUERY_PORT: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("doris.feQueryPort")
      .doc("Doris FE MySQL port used for JDBC queries against information_schema and ADMIN commands")
      .version("2.4")
      .intConf
      .createWithDefault(9030)

  val DORIS_OVERWRITE: ConfigEntryWithDefault[Boolean] =
    PistaConfigBuilder("doris.overwrite")
      .doc("Request atomic overwrite; output.mode=overwrite also enables this path. A non-empty partitionDate atomically replaces the selected partition " +
        "through a temporary partition and REPLACE PARTITION; an empty partitionDate replaces the entire table through ALTER TABLE REPLACE.")
      .version("2.5")
      .booleanConf
      .createWithDefault(false)

  val DORIS_PARTITION_DATE: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.partition.dateValue")
      .doc("Date partition value (for example, 20260407), used to locate the target partition when overwrite=true; " +
        "supports comma-separated values (for example, 20260407,20260408); empty overwrites the entire table")
      .version("2.4")
      .stringConf
      .createWithDefault("")

  val DORIS_PARTITION_COLUMN: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.partition.columnName")
      .doc("Date partition column (for example, partition_date). Used with partition.dateValue " +
        "to filter source data only when overwrite=true and partitionDate is non-empty; " +
        "empty means no source filter")
      .version("2.4")
      .stringConf
      .createWithDefault("")

  val DORIS_PARTITION_DATE_FORMAT: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("doris.partition.dateFormat")
      .doc("Date format used to convert a StringType partition column to Doris DATE before writing." +
        "defaults to yyyyMMdd; used only when overwrite=true and partitionDate is non-empty")
      .version("2.4")
      .stringConf
      .createWithDefault("yyyyMMdd")

  val entries: Seq[ConfigEntry[_]] = Seq(
    DORIS_FENODES, DORIS_DATABASE, DORIS_TABLE,
    DORIS_CLUSTER_NAME,
    DORIS_USER, DORIS_PASSWORD, DORIS_WRITE_MODE,
    DORIS_OUTPUT_PARTITIONS, DORIS_LABEL_PREFIX,
    DORIS_CONNECTOR_BATCH_SIZE, DORIS_CONNECTOR_FORMAT, DORIS_CONNECTOR_ENABLE_2PC,
    DORIS_AUTO_REDIRECT, DORIS_BENODES, DORIS_FE_QUERY_PORT,
    DORIS_OVERWRITE, DORIS_PARTITION_DATE, DORIS_PARTITION_COLUMN, DORIS_PARTITION_DATE_FORMAT
  )
}
