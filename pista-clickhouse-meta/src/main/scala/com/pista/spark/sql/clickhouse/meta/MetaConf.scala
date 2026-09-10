package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.sql.conf.SubmitterConf

/**
 * PostgreSQL metadata database connection configuration.
 *
 * @param host     Meta database host
 * @param port     Port (default PostgreSQL port is 5432)
 * @param database Database name
 * @param username username
 * @param password Password
 * @param schema   PostgreSQL schema (default: public)
 */
case class MetaConf(
  host: String,
  port: Int,
  database: String,
  username: String,
  password: String,
  schema: String = ""
) {
  def postgresUrl: String = {
    val schemaName = if (schema.nonEmpty) schema else "public"
    s"jdbc:postgresql://$host:$port/$database?" +
      s"currentSchema=$schemaName&options=--client_encoding=UTF8"
  }
}

object MetaConf {
  /** Read metadata configuration from Spark conf */
  def fromSparkConf(conf: org.apache.spark.SparkConf): MetaConf =
    MetaConf(
      host     = conf.get(SubmitterConf.META_HOST.key),
      port     = conf.getInt(SubmitterConf.META_PORT.key, SubmitterConf.META_PORT.defaultValue.get),
      database = conf.get(SubmitterConf.META_DATABASE.key),
      username = conf.get(SubmitterConf.META_USERNAME.key),
      password = conf.get(SubmitterConf.META_PASSWORD.key),
      schema   = conf.get(SubmitterConf.META_SCHEMA.key, SubmitterConf.META_SCHEMA.defaultValue.get)
    )
}
