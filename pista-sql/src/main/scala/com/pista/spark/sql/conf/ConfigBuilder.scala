package com.pista.spark.sql.conf

/**
 * Configure cascading builder
 *
 * Usage examples: * Usage examples:
 * {{{
 * val MY_CONFIG = ConfigBuilder("spark.pista.myConfig")
 *   .doc("configuration explanation")
 *   .version("1.0.0")
 *   .stringConf
 *   .createWithDefault("default")
 * }}}
 *
 */
class ConfigBuilder(val key: String) {
  private var _doc: String = ""
  private var _version: String = "1.0.0"
  private var _alternatives: Seq[String] = Seq.empty

  def doc(s: String): ConfigBuilder = { _doc = s; this }
  def version(v: String): ConfigBuilder = { _version = v; this }
  def alternatives(keys: String*): ConfigBuilder = { _alternatives = keys; this }

  def stringConf: TypedConfigBuilder[String] =
    new TypedConfigBuilder(this, _doc, _version, _alternatives, identity, identity)

  def intConf: TypedConfigBuilder[Int] =
    new TypedConfigBuilder(this, _doc, _version, _alternatives, _.toInt, _.toString)

  def longConf: TypedConfigBuilder[Long] =
    new TypedConfigBuilder(this, _doc, _version, _alternatives, _.toLong, _.toString)

  def doubleConf: TypedConfigBuilder[Double] =
    new TypedConfigBuilder(this, _doc, _version, _alternatives, _.toDouble, _.toString)

  def booleanConf: TypedConfigBuilder[Boolean] =
    new TypedConfigBuilder(this, _doc, _version, _alternatives, _.toBoolean, _.toString)

  def stringSeqConf: TypedConfigBuilder[Seq[String]] =
    new TypedConfigBuilder(
      this, _doc, _version, _alternatives,
      s => if (s.isEmpty) Seq.empty else s.split(",").map(_.trim).toSeq,
      _.mkString(",")
    )
}

/**
 * Type Builder Configuration Builder
 */
class TypedConfigBuilder[T](
  parent: ConfigBuilder,
  doc: String,
  version: String,
  alternatives: Seq[String],
  converter: String => T,
  stringifier: T => String
) {
  def createWithDefault(default: T): ConfigEntryWithDefault[T] =
    new ConfigEntryWithDefault(
      parent.key, doc, version, default,
      converter, stringifier, alternatives
    )

  def createOptional: OptionalConfigEntry[T] =
    new OptionalConfigEntry(
      parent.key, doc, version,
      converter, stringifier, alternatives
    )
}

object ConfigBuilder {
  def apply(key: String) = new ConfigBuilder(key)
}

/** Builder for Pista-owned Spark runtime configuration. */
object PistaConfigBuilder {
  val PREFIX: String = "spark.pista."

  def apply(suffix: String): ConfigBuilder =
    ConfigBuilder(s"$PREFIX$suffix")
}
