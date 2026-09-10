package com.pista.spark.sql.conf

import com.pista.spark.errors.PistaErrors
import org.apache.spark.sql.{RuntimeConfig, SparkSession}

/**
 * configure reading utility class
 *
 * Provide type-safe configuration read methods supporting default values and optional configurations.
 * Create from SparkSession, RuntimeConfig, or Map[String, String].
 *
 * Usage examples: * Usage examples:
 * {{{
 * // Read from SparkSession.
 * val conf = ConfigReader(sparkSession)
 * val sqlFile = conf.require(SubmitterConf.SQL_FILE)
 * val logLevel = conf.get(SubmitterConf.LOG_LEVEL)
 *
 * // From Map (Processor Scenario)
 * val conf = ConfigReader(context.config)
 * val outputFormat = conf.get(SubmitterConf.OUTPUT_FORMAT)
 * }}}
 *
 */
class ConfigReader private (
  private val lookup: String => Option[String],
  private val allEntries: () => Map[String, String]
) {

  /** Read the primary key, then any explicitly declared alternative. */
  private def resolve[T](entry: ConfigEntry[T]): Option[T] =
    lookup(entry.key)
      .orElse(entry.alternatives.collectFirst(Function.unlift(lookup)))
      .map(entry.parse)

  /** Get configuration value (with default value) */
  def get[T](entry: ConfigEntryWithDefault[T]): T =
    resolve(entry).getOrElse(entry._defaultValue)

  /** Get configuration value (optional configuration item) */
  def get[T](entry: OptionalConfigEntry[T]): Option[T] =
    resolve(entry)

  /** Retrieve all configurations below a prefix, returning keys with the prefix removed. */
  def getAllWithPrefix(prefix: String): Map[String, String] =
    allEntries()
      .filter { case (k, _) => k.startsWith(prefix) && k.length > prefix.length }
      .map { case (k, v) => k.substring(prefix.length) -> v }

  /** Check if mandatory configurations exist, throw an exception if not */
  def require[T](entry: OptionalConfigEntry[T]): T =
    get(entry).getOrElse {
      throw PistaErrors.missingRequiredConfigError(entry.key)
    }

  /** Obtain the original string value of the configuration item */
  def getOption(key: String): Option[String] = lookup(key)

  /** Check if configuration items are set */
  def contains(entry: ConfigEntry[_]): Boolean =
    lookup(entry.key).isDefined ||
      entry.alternatives.exists(lookup(_).isDefined)
}

object ConfigReader {
  def apply(spark: SparkSession): ConfigReader = apply(spark.conf)
  def apply(conf: RuntimeConfig): ConfigReader =
    new ConfigReader(conf.getOption, () => conf.getAll)
  def apply(map: Map[String, String]): ConfigReader =
    new ConfigReader(map.get, () => map)
}
