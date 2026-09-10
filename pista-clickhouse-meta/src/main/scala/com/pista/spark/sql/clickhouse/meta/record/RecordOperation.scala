package com.pista.spark.sql.clickhouse.meta.record

object RecordOperation extends Enumeration {
  val CREATE_RECORD, SEARCH_RECORD, DELETE_RECORD = Value
  val SUM_DATA_VOLUME: Value = Value
  val UPDATE_HOST_ADDRESS, UPDATE_TASK_STATUS, UPDATE_DATA_VOLUME = Value
}
