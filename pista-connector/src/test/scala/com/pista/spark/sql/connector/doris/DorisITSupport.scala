package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.doris.batch._
import com.pista.spark.sql.test.base.DorisBaseIT
import com.pista.spark.sql.test.util.TestDataGenerator
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

trait DorisITSupport extends DorisBaseIT {

  protected def createDorisTable(prefix: String, definition: String): (String, String) = {
    val (database, table) = uniqueDorisTable(prefix)
    executeDoris(s"CREATE DATABASE IF NOT EXISTS `$database`")
    executeDoris(s"CREATE TABLE `$database`.`$table` $definition")
    (database, table)
  }

  protected def twoColumnSource(ids: Seq[Int], valuePrefix: String = "value"): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(ids.map(id => Row(id, s"$valuePrefix-$id")), 2),
      StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false))))

  protected def partitionSource(rows: Seq[(Int, Int, String)]): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        rows.map { case (id, bizDate, value) => Row(id, bizDate, value) }, 2),
      StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("biz_date", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false))))

  protected def batchConfig(
    database: String,
    table: String,
    labelPrefix: String = TestDataGenerator.uniqueName("pista_it_doris"),
    writeMode: WriteMode = WriteMode.SPARK_CONNECTOR,
    enable2PC: Boolean = false,
    overwrite: Boolean = false,
    partitionDate: String = "",
    partitionColumn: String = ""
  ): DorisBatchConfig =
    DorisBatchConfig(
      fenodes = fenodes,
      database = database,
      table = table,
      user = "root",
      password = "",
      writeMode = writeMode,
      labelPrefix = labelPrefix,
      outputPartitions = 2,
      batchSize = 100000,
      dataFormat = "csv",
      enable2PC = enable2PC,
      autoRedirect = false,
      benodes = benodes.mkString(","),
      feQueryPort = feQueryPort,
      overwrite = overwrite,
      partitionDate = partitionDate,
      partitionColumn = partitionColumn)

  protected def failingAfterPreparationWriter(
    config: DorisBatchConfig,
    source: DataFrame,
    jobTimestamp: Long
  ): DorisConcurrentWriter =
    new DorisConcurrentWriter(
      config,
      DorisConcurrentContext(clusterName = None, metaManager = None),
      source,
      jobTimestamp) {
      override protected def createConnectorWriter(
        writeConfig: DorisBatchConfig,
        sourceDf: DataFrame,
        timestamp: Long
      ): SparkDorisConnectorWriter =
        new SparkDorisConnectorWriter(writeConfig, sourceDf, timestamp) {
          override def write(): BatchWriteResult =
            throw new IllegalStateException("injected connector failure")
        }
    }

  protected def executeDoris(sql: String): Unit =
    withDorisConnection { connection =>
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    }

  protected def queryLong(sql: String): Long =
    queryColumn(sql)(_.getLong(1)).headOption.getOrElse(
      fail(s"The query did not return any results.: $sql"))

  protected def queryColumn[T](sql: String)(read: ResultSet => T): Seq[T] =
    withDorisConnection { connection =>
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(sql)
        try {
          val values = ListBuffer.empty[T]
          while (result.next()) values += read(result)
          values.toSeq
        } finally result.close()
      } finally statement.close()
    }

  protected def tableExists(database: String, table: String): Boolean =
    queryColumn(s"SHOW TABLES FROM `$database` LIKE '$table'")(_.getString(1)).nonEmpty

  protected def temporaryPartitionNames(database: String, table: String): Seq[String] =
    queryColumn(s"SHOW TEMPORARY PARTITIONS FROM `$database`.`$table`")(
      _.getString("PartitionName"))
}
