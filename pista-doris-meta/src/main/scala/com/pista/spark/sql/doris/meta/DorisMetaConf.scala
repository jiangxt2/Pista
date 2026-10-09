package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import org.apache.spark.SparkConf

/**
 * Doris metadata management configuration
 *
 * @param pgHost     PostgreSQL Host
 * @param pgPort     PostgreSQL Port
 * @param pgDatabase PostgreSQL Database Name
 * @param pgUsername PostgreSQL Username
 * @param pgPassword PostgreSQL Password
 * @param clusterName Doris Cluster Name (logical identifier, associated with cluster_info table)
 *
 */
case class DorisMetaConf(
  pgHost:     String,
  pgPort:     Int,
  pgDatabase: String,
  pgUsername: String,
  pgPassword: String,
  clusterName: String
)

object DorisMetaConf {
  private val requiredKeys = Seq(
    SubmitterConf.META_HOST.key,
    SubmitterConf.META_DATABASE.key,
    SubmitterConf.META_USERNAME.key,
    SubmitterConf.META_PASSWORD.key,
    SubmitterConf.DORIS_CLUSTER_NAME.key)

  /** Cluster identity alone does not request a PostgreSQL metadata store. */
  def isConfigured(conf: ConfigReader): Boolean =
    conf.getAllWithPrefix("spark.pista.meta.").nonEmpty

  def missingKeys(conf: ConfigReader): Seq[String] =
    requiredKeys.filter { key =>
      conf.getOption(key).forall(value =>
        key != SubmitterConf.META_PASSWORD.key && value.trim.isEmpty)
    }

  def fromConfigReader(conf: ConfigReader): DorisMetaConf = {
    val missing = missingKeys(conf)
    if (missing.nonEmpty)
      throw new NoSuchElementException(s"Missing Doris metadata configuration keys: ${missing.mkString(", ")}")

    DorisMetaConf(
      pgHost = conf.get(SubmitterConf.META_HOST).get,
      pgPort = conf.get(SubmitterConf.META_PORT),
      pgDatabase = conf.get(SubmitterConf.META_DATABASE).get,
      pgUsername = conf.get(SubmitterConf.META_USERNAME).get,
      pgPassword = conf.get(SubmitterConf.META_PASSWORD).get,
      clusterName = conf.get(SubmitterConf.DORIS_CLUSTER_NAME).get)
  }

  /**
   * Create metadata configuration from SparkConf
   */
  def fromSparkConf(conf: SparkConf): DorisMetaConf =
    fromConfigReader(ConfigReader(conf.getAll.toMap))
}
