package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.conf.SubmitterConf
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
  /**
   * Create metadata configuration from SparkConf
   */
  def fromSparkConf(conf: SparkConf): DorisMetaConf =
    DorisMetaConf(
      pgHost      = conf.get(SubmitterConf.META_HOST.key),
      pgPort      = conf.getInt(SubmitterConf.META_PORT.key, SubmitterConf.META_PORT.defaultValue.get),
      pgDatabase  = conf.get(SubmitterConf.META_DATABASE.key),
      pgUsername  = conf.get(SubmitterConf.META_USERNAME.key),
      pgPassword  = conf.get(SubmitterConf.META_PASSWORD.key),
      clusterName = conf.get(SubmitterConf.DORIS_CLUSTER_NAME.key)
    )
}
