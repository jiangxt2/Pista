package com.pista.spark.sql.conf

/**
 * Configuration class for defining items
 *
 * @tparam T Configuration value type
 *
 */
sealed abstract class ConfigEntry[T](
  val key: String,
  val doc: String,
  val version: String,
  val alternatives: Seq[String] = Seq.empty
) {
  def defaultValue: Option[T]
  def valueConverter: String => T
  def stringConverter: T => String

  /** Parse configuration values from string */
  def parse(value: String): T = valueConverter(value)

  /** Convert configuration values to strings */
  def stringify(value: T): String = stringConverter(value)

  override def toString: String = s"ConfigEntry(key=$key, doc=$doc, version=$version)"
}

/**
 * Configurations with default values
 */
class ConfigEntryWithDefault[T](
  key: String,
  doc: String,
  version: String,
  val _defaultValue: T,
  val valueConverter: String => T,
  val stringConverter: T => String,
  alternatives: Seq[String] = Seq.empty
) extends ConfigEntry[T](key, doc, version, alternatives) {
  override def defaultValue: Option[T] = Some(_defaultValue)
}

/**
 * Optional Configuration Items (No Default Values)
 */
class OptionalConfigEntry[T](
  key: String,
  doc: String,
  version: String,
  val valueConverter: String => T,
  val stringConverter: T => String,
  alternatives: Seq[String] = Seq.empty
) extends ConfigEntry[T](key, doc, version, alternatives) {
  override def defaultValue: Option[T] = None
}
