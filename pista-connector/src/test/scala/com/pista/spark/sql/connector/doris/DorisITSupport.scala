package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.connector.doris.batch._
import com.pista.spark.sql.conf.SubmitterConf
import com.pista.spark.sql.doris.meta.{DorisMetaConf, DorisMetaConnection, DorisMetaManager}
import com.pista.spark.sql.test.base.DorisBaseIT
import com.pista.spark.sql.test.util.{JdbcAssertUtils, TestDataGenerator}
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
    jobTimestamp: Long,
    context: DorisConcurrentContext = DorisConcurrentContext(None, None)
  ): DorisConcurrentWriter =
    new DorisConcurrentWriter(
      config,
      context,
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

  protected def withMetadataContext[T](cluster: String = "pista_it")(
    operation: DorisConcurrentContext => T
  ): T = {
    val conf = DorisMetaConf(pgContainer.getHost, pgContainer.getMappedPort(5432), "pista_meta",
      metaUser, metaPassword, cluster)
    val manager = new DorisMetaManager(new DorisMetaConnection(conf), conf)
    try operation(DorisConcurrentContext(Some(cluster), Some(manager)))
    finally manager.close()
  }

  protected def expectedTaskId(config: DorisBatchConfig, cluster: String = "pista_it"): String =
    new TestableDorisConcurrentWriter(config, DorisConcurrentContext(Some(cluster), None), spark)
      .exposedBuildTaskId(config, Some(cluster))

  protected def assertTaskState(
    config: DorisBatchConfig,
    status: Int,
    writtenRows: Long,
    cluster: String = "pista_it"
  ): Unit =
    JdbcAssertUtils.withConnection(metaJdbcUrl, metaUser, metaPassword) { connection =>
      val statement = connection.prepareStatement(
        "SELECT task_id, status, written_rows, rdate, partition_filter FROM doris_task_info WHERE task_id = ?")
      try {
        statement.setString(1, expectedTaskId(config, cluster))
        val result = statement.executeQuery()
        try {
          assert(result.next())
          assert(DorisMetaManager.characterCount(result.getString("task_id")) <= 256)
          assert(result.getInt("status") == status)
          assert(result.getLong("written_rows") == writtenRows)
          assert(result.getString("rdate") == config.partitionDate)
          assert(result.getString("partition_filter") == config.resolvePartitionFilter().orNull)
          assert(!result.next())
        } finally result.close()
      } finally statement.close()
    }

  protected def withWriterConfiguration[T](config: DorisBatchConfig)(operation: => T): T = {
    val values = Map(
      SubmitterConf.DORIS_FENODES.key -> config.fenodes,
      SubmitterConf.DORIS_FE_QUERY_PORT.key -> config.feQueryPort.toString,
      SubmitterConf.DORIS_DATABASE.key -> config.database,
      SubmitterConf.DORIS_TABLE.key -> config.table,
      SubmitterConf.DORIS_LABEL_PREFIX.key -> config.labelPrefix,
      SubmitterConf.DORIS_OUTPUT_PARTITIONS.key -> config.outputPartitions.toString,
      SubmitterConf.DORIS_AUTO_REDIRECT.key -> config.autoRedirect.toString,
      SubmitterConf.DORIS_BENODES.key -> config.benodes,
      SubmitterConf.DORIS_OVERWRITE.key -> config.overwrite.toString,
      SubmitterConf.DORIS_PARTITION_DATE.key -> config.partitionDate,
      SubmitterConf.DORIS_PARTITION_COLUMN.key -> config.partitionColumn,
      SubmitterConf.DORIS_CLUSTER_NAME.key -> "pista_it",
      SubmitterConf.META_HOST.key -> pgContainer.getHost,
      SubmitterConf.META_PORT.key -> pgContainer.getMappedPort(5432).toString,
      SubmitterConf.META_DATABASE.key -> "pista_meta",
      SubmitterConf.META_USERNAME.key -> metaUser,
      SubmitterConf.META_PASSWORD.key -> metaPassword)
    val previous = values.keys.map(key => key -> spark.conf.getOption(key)).toMap
    values.foreach { case (key, value) => spark.conf.set(key, value) }
    try operation
    finally previous.foreach {
      case (key, Some(value)) => spark.conf.set(key, value)
      case (key, None) => spark.conf.unset(key)
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
