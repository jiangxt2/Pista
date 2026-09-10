package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.conf.SubmitterConf
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

class DorisMetaConfSuite extends AnyFunSuite {

  test("From correct parsing of SparkConf to DorisMetaConf") {
    val conf = new SparkConf()
      .set(SubmitterConf.META_HOST.key, "127.0.0.1")
      .set(SubmitterConf.META_PORT.key, "5432")
      .set(SubmitterConf.META_DATABASE.key, "doris_meta")
      .set(SubmitterConf.META_USERNAME.key, "admin")
      .set(SubmitterConf.META_PASSWORD.key, "secret")
      .set(SubmitterConf.DORIS_CLUSTER_NAME.key, "test_cluster")

    val meta = DorisMetaConf.fromSparkConf(conf)
    assert(meta.pgHost === "127.0.0.1")
    assert(meta.pgPort === 5432)
    assert(meta.pgDatabase === "doris_meta")
    assert(meta.pgUsername === "admin")
    assert(meta.pgPassword === "secret")
    assert(meta.clusterName === "test_cluster")
  }

  test("missing required configuration throws NoSuchElementException") {
    val conf = new SparkConf()
      .set(SubmitterConf.META_HOST.key, "127.0.0.1")

    val ex = intercept[NoSuchElementException] {
      DorisMetaConf.fromSparkConf(conf)
    }
    assert(ex.getMessage.contains(SubmitterConf.META_DATABASE.key))
  }

  test("port uses default value") {
    val conf = new SparkConf()
      .set(SubmitterConf.META_HOST.key, "127.0.0.1")
      .set(SubmitterConf.META_DATABASE.key, "doris_meta")
      .set(SubmitterConf.META_USERNAME.key, "admin")
      .set(SubmitterConf.META_PASSWORD.key, "secret")
      .set(SubmitterConf.DORIS_CLUSTER_NAME.key, "test_cluster")

    val meta = DorisMetaConf.fromSparkConf(conf)
    assert(meta.pgPort === SubmitterConf.META_PORT.defaultValue.get)
  }
}
