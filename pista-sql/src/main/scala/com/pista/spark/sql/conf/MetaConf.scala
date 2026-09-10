package com.pista.spark.sql.conf

/**
 * General Meta Configuration
 *
 * Contains PostgreSQL Metadata database connection configuration for batch metadata tracking of ClickHouse/Doris.
 */
private[sql] object MetaConf {

  val META_HOST: OptionalConfigEntry[String] =
    PistaConfigBuilder("meta.host")
      .doc("Meta PostgreSQL host")
      .version("2.4")
      .stringConf
      .createOptional

  val META_PORT: ConfigEntryWithDefault[Int] =
    PistaConfigBuilder("meta.port")
      .doc("Meta PostgreSQL port")
      .version("2.4")
      .intConf
      .createWithDefault(5432)

  val META_DATABASE: OptionalConfigEntry[String] =
    PistaConfigBuilder("meta.database")
      .doc("Meta PostgreSQL databaseName")
      .version("2.4")
      .stringConf
      .createOptional

  val META_USERNAME: OptionalConfigEntry[String] =
    PistaConfigBuilder("meta.username")
      .doc("Meta PostgreSQL Username")
      .version("2.4")
      .stringConf
      .createOptional

  val META_PASSWORD: OptionalConfigEntry[String] =
    PistaConfigBuilder("meta.password")
      .doc("Meta PostgreSQL Password")
      .version("2.4")
      .stringConf
      .createOptional

  val META_SCHEMA: ConfigEntryWithDefault[String] =
    PistaConfigBuilder("meta.schema")
      .doc("Meta PostgreSQL schema")
      .version("2.4")
      .stringConf
      .createWithDefault("")

  val entries: Seq[ConfigEntry[_]] = Seq(
    META_HOST, META_PORT, META_DATABASE, META_USERNAME, META_PASSWORD, META_SCHEMA
  )
}
