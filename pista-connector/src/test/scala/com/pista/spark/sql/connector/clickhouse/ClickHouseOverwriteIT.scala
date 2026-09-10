package com.pista.spark.sql.connector.clickhouse

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import java.sql.Date

class ClickHouseOverwriteIT extends ClickHouseITSupport {
  test("ClickHouse overwrite on_cluster and per_host both replace the target partition") {
    val configKeys = Seq(
      "spark.pista.clickhouse.overwrite",
      "spark.pista.clickhouse.overwrite.mode",
      "spark.pista.clickhouse.partition.dateValue",
      "spark.pista.clickhouse.partition.columnName")
    val originalConfigs = configKeys.map(key => key -> spark.conf.getOption(key))

    try {
      Seq("on_cluster", "per_host").foreach { mode =>
        val (database, table) = uniqueClickHouseTable(s"it_ck_overwrite_$mode")
        ckContainer.executeOnEntry(
          s"""CREATE TABLE $database.$table ON CLUSTER ck_cluster
             |(id Int32, value String, biz_date Date)
             |ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$table', '{replica}')
             |PARTITION BY biz_date
             |ORDER BY id""".stripMargin)

        (0 until ckContainer.nodeCount).foreach { node =>
          ckContainer.executeOn(
            node,
            s"""INSERT INTO $database.$table VALUES
               |(${node + 1}, 'keep-$node', '2026-08-02'),
               |(${1000 + node}, 'old-$node', '2026-08-01')""".stripMargin)
        }

        spark.conf.set("spark.pista.clickhouse.overwrite", "true")
        spark.conf.set("spark.pista.clickhouse.overwrite.mode", mode)
        spark.conf.set("spark.pista.clickhouse.partition.dateValue", "2026-08-01")
        spark.conf.set("spark.pista.clickhouse.partition.columnName", "biz_date")

        val ids = 101 to 109
        write(datedSource(ids, "2026-08-01"), writerConfig(database, table))

        val targetRows = (0 until ckContainer.nodeCount).map { node =>
          ckContainer.queryLong(
            node,
            s"SELECT count() FROM $database.$table WHERE biz_date = toDate('2026-08-01')")
        }.sum
        val retainedRows = (0 until ckContainer.nodeCount).map { node =>
          ckContainer.queryLong(
            node,
            s"SELECT count() FROM $database.$table WHERE biz_date = toDate('2026-08-02')")
        }.sum
        val oldRows = (0 until ckContainer.nodeCount).map { node =>
          ckContainer.queryLong(
            node,
            s"SELECT count() FROM $database.$table WHERE value LIKE 'old-%'")
        }.sum

        assert(targetRows == ids.size)
        assert(retainedRows == ckContainer.nodeCount)
        assert(oldRows == 0L)
        assertExactShardRows(
          database,
          table,
          ids,
          rDate = "2026-08-01",
          rowCondition = "biz_date = toDate('2026-08-01')")
      }
    } finally {
      originalConfigs.foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None)        => spark.conf.unset(key)
      }
    }
  }

  private def datedSource(ids: Seq[Int], date: String): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        ids.map(id => Row(id, s"new-$id", Date.valueOf(date))), 3),
      StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false),
        StructField("biz_date", DateType, nullable = false))))
}
