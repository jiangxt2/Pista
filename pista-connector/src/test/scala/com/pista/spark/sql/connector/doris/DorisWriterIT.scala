package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import com.pista.spark.sql.test.base.DorisBaseIT
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/** End-to-end Doris writer test using the official FE/BE images. */
class DorisWriterIT extends DorisBaseIT {

  test("Doris writer writes and can be queried via FE JDBC") {
    val (database, table) = uniqueDorisTable("it_doris_writer")
    withDorisConnection { connection =>
      val statement = connection.createStatement()
      try {
        statement.execute(s"CREATE DATABASE IF NOT EXISTS $database")
        statement.execute(
          s"""CREATE TABLE $database.$table
             |(id INT, value VARCHAR(128))
             |DUPLICATE KEY(id)
             |DISTRIBUTED BY HASH(id) BUCKETS 2
             |PROPERTIES ("replication_num" = "2")""".stripMargin)
      } finally statement.close()
    }

    val source = spark.createDataFrame(
      spark.sparkContext.parallelize((1 to 20).map(index => Row(index, s"value-$index")), 2),
      StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false))))

    WriterRegistry.write(source, OutputConfig(
      path = Some(s"$database.$table"),
      format = "doris",
      mode = "append",
      partitionBy = Seq.empty,
      options = Map(
        "doris.fenodes" -> fenodes,
        "doris.query.port" -> feQueryPort.toString,
        "doris.user" -> "root",
        "doris.password" -> "",
        "force.jdbc" -> "true")))

    withDorisConnection { connection =>
      val result = connection.createStatement().executeQuery(
        s"SELECT COUNT(*) FROM $database.$table")
      try {
        assert(result.next())
        assert(result.getLong(1) == 20L)
      } finally result.close()
    }
  }
}
