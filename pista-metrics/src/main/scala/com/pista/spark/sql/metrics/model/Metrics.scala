package com.pista.spark.sql.metrics.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * MetricsBase trait
 *
 */
trait Metrics {
  def metricType: String
  def appId: String
  def collectTimestamp: String
}

object Metrics {
  private val mapper  = new ObjectMapper().registerModule(DefaultScalaModule)
  private val fmt     = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def toJson(metrics: Metrics): String = mapper.writeValueAsString(metrics)

  def formatTime(ms: Long): String =
    java.time.Instant.ofEpochMilli(ms)
      .atZone(java.time.ZoneId.systemDefault())
      .format(fmt)
}
