package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.clickhouse.meta.{MetaConf, MetaConnection, MetaManager}
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import com.pista.spark.sql.test.base.ClickHouseBaseIT
import com.pista.spark.sql.test.util.JdbcAssertUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

private[clickhouse] final case class ClickHouseShardMeta(
  dataIndex: Int,
  hostAddress: String,
  originalDataVolume: Long,
  insertDataVolume: Long,
  status: Int)

/** Shared setup for ClickHouse writer ITs; all identifiers are run-unique. */
trait ClickHouseITSupport extends ClickHouseBaseIT {
  protected def prepareTable(columns: String, prefix: String): (String, String) = {
    val (database, table) = uniqueClickHouseTable(prefix)
    ckContainer.executeOnEntry(
      s"""CREATE TABLE $database.$table ON CLUSTER ck_cluster
         |($columns)
         |ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$table', '{replica}')
         |ORDER BY id""".stripMargin)
    (database, table)
  }

  protected def writerConfig(database: String, table: String): OutputConfig = {
    spark.conf.set("spark.pista.clickhouse.machine.count", "3")
    spark.conf.set("spark.pista.clickhouse.primaryKey", "id")
    spark.conf.set("spark.pista.clickhouse.clusterName", "ck_cluster")
    spark.conf.set("spark.pista.clickhouse.port", ckContainer.nodeAddresses.head.split(":").last)
    spark.conf.set("spark.pista.meta.host", pgContainer.getHost)
    spark.conf.set("spark.pista.meta.port", pgContainer.getMappedPort(5432).toString)
    spark.conf.set("spark.pista.meta.database", "pista_meta")
    spark.conf.set("spark.pista.meta.username", metaUser)
    spark.conf.set("spark.pista.meta.password", metaPassword)
    OutputConfig(
      path = Some(s"$database.$table"),
      format = "clickhouse",
      mode = "append",
      partitionBy = Seq.empty,
      options = Map(
        "clickhouse.username" -> ckContainer.username,
        "clickhouse.password" -> ckContainer.userPassword,
        "socket_timeout" -> "120000"))
  }

  protected def sourceRows(values: Seq[(Int, String)]): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(values.map { case (id, value) => Row(id, value) }, 3),
      StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false))))

  protected def write(source: DataFrame, config: OutputConfig): Unit =
    WriterRegistry.write(source, config)

  protected def nodeCounts(database: String, table: String): Seq[Long] =
    (0 until ckContainer.nodeCount).map(index =>
      ckContainer.queryLong(index, s"SELECT count() FROM $database.$table"))

  protected def withMetaManager[T](f: MetaManager => T): T = {
    val connection = new MetaConnection(MetaConf(
      host = pgContainer.getHost,
      port = pgContainer.getMappedPort(5432),
      database = "pista_meta",
      username = metaUser,
      password = metaPassword))
    val manager = new MetaManager(connection)
    try f(manager)
    finally manager.close()
  }

  protected def shardMetaRecords(
    database: String,
    table: String,
    rDate: String = ""
  ): Seq[ClickHouseShardMeta] =
    JdbcAssertUtils.withConnection(metaJdbcUrl, metaUser, metaPassword) { connection =>
      val statement = connection.prepareStatement(
        """SELECT data_index, host_address, original_data_volume,
          |       insert_data_volume, status
          |FROM clickhouse_data_records
          |WHERE dbname = ? AND tbname = ? AND rdate = ?
          |  AND status != -2
          |ORDER BY data_index""".stripMargin)
      try {
        statement.setString(1, database)
        statement.setString(2, table)
        statement.setString(3, rDate)
        val result = statement.executeQuery()
        try {
          val records = ListBuffer.empty[ClickHouseShardMeta]
          while (result.next()) {
            records += ClickHouseShardMeta(
              dataIndex = result.getInt(1),
              hostAddress = result.getString(2),
              originalDataVolume = result.getLong(3),
              insertDataVolume = result.getLong(4),
              status = result.getInt(5))
          }
          records.toSeq
        } finally result.close()
      } finally statement.close()
    }

  protected def expectedShard(id: Int): Int =
    Math.abs(id.toString.hashCode) % ckContainer.nodeCount

  protected def assertExactShardRows(
    database: String,
    table: String,
    ids: Seq[Int],
    rDate: String = "",
    rowCondition: String = ""
  ): Unit = {
    val records = shardMetaRecords(database, table, rDate)
    assert(records.map(_.dataIndex).toSet == (0 until ckContainer.nodeCount).toSet)

    records.foreach { record =>
      val nodeIndex = clickHouseNodes.indexOf(record.hostAddress)
      assert(nodeIndex >= 0, s"Meta host does not match any test node: ${record.hostAddress}")
      val expected = ids.filter(expectedShard(_) == record.dataIndex)
      val where = if (rowCondition.nonEmpty) s" WHERE $rowCondition" else ""
      val actual = ckContainer.queryLong(
        nodeIndex, s"SELECT count() FROM $database.$table$where")
      assert(actual == expected.size,
        s"shard=${record.dataIndex}, expected=${expected.mkString(",")}, actual=$actual")

      val matching = if (expected.isEmpty) 0L else {
        val prefix = if (rowCondition.nonEmpty) s"$rowCondition AND " else ""
        ckContainer.queryLong(
          nodeIndex,
          s"SELECT count() FROM $database.$table WHERE ${prefix}id IN (${expected.mkString(",")})")
      }
      assert(matching == expected.size)
    }
  }
}
