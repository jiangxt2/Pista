package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.writer.WriterRegistry
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ClickHouseNullConversionIT extends ClickHouseITSupport {
  test("ClickHouse convertNullToDefault write after converting null values to default") {
    val (database, table) = prepareTable("id Int32, value String, amount Int32", "it_ck_null")
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = true),
      StructField("amount", IntegerType, nullable = true)))
    val source = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, null, null), Row(2, "ok", 7)), 2), schema)
    val config = writerConfig(database, table)
    spark.conf.set("spark.pista.clickhouse.convertNullToDefault", "true")
    WriterRegistry.write(source, config)

    assert(nodeCounts(database, table).sum == 2L)
    val defaultRows = (0 until ckContainer.nodeCount).map(index =>
      ckContainer.queryLong(index,
        s"SELECT count() FROM $database.$table WHERE value = '' AND amount = 0")).sum
    assert(defaultRows == 1L)
  }
}
