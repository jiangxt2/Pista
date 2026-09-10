package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.sql.clickhouse.meta.record.{DataInfoRecord, DataRecord, RecordStatus}
import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.{JdbcAssertUtils, MetaSchemaInitializer, TestDataGenerator}

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

class ClickHouseMetaManagerIT extends BaseIT {

  test("MetaConnection reconnects after the underlying connection closes") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)

    val connection = newConnection()
    try {
      val original = connection.getConnection
      original.close()
      val replacement = connection.getConnection
      assert(replacement ne original)
      assert(!replacement.isClosed)
      assert(new MetaManager(connection).getClusterIdByName("ck_cluster") > 0)
    } finally connection.close()
  }

  test("MetaManager completes real CRUD for DataInfo and DataRecord, and aggregates the data volume") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)

    val connection = newConnection()
    val manager = new MetaManager(connection)
    val table = TestDataGenerator.uniqueName("meta_it_task")
    try {
      val clusterId = manager.getClusterIdByName("ck_cluster")
      val info = manager.verifyDataInfoRecord(DataInfoRecord(
        dbName = "pista_test",
        tbName = table,
        rDate = "",
        clusterId = clusterId,
        indexSize = 3), overwrite = false).getOrElse(
        fail("failed to create DataInfoRecord"))

      val shard = manager.verifyDataRecord(DataRecord(
        dbName = "pista_test",
        tbName = table,
        rDate = "",
        clusterId = clusterId,
        dataIndex = 0,
        indexSize = 3,
        hostAddress = "127.0.0.1:8123"), overwrite = false).getOrElse(
        fail("failed to create DataRecord"))

      val completedShard = shard.copy(
        originalDataVolume = 7L,
        insertDataVolume = 7L,
        status = RecordStatus.SUCCESS_VALUE)
      assert(manager.updateDataVolume(completedShard))
      assert(manager.updateTaskStatus(completedShard))

      val storedShard = manager.searchRecord(completedShard).get
      assert(storedShard.status == RecordStatus.SUCCESS_VALUE)
      assert(storedShard.insertDataVolume == 7L)
      assert(manager.sumDataVolume(info) == 7L)

      val completedInfo = info.copy(
        dataVolume = 7L,
        status = RecordStatus.SUCCESS_VALUE)
      assert(manager.updateDataVolume(completedInfo))
      assert(manager.updateTaskStatus(completedInfo))
      val storedInfo = manager.searchRecord(completedInfo).get
      assert(storedInfo.status == RecordStatus.SUCCESS_VALUE)
      assert(storedInfo.dataVolume == 7L)
    } finally manager.close()
  }

  test("MetaManager concurrent validation of the same logical record to maintain uniqueness and complete the state transition") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)

    val manager = new MetaManager(newConnection())
    val table = TestDataGenerator.uniqueName("meta_it_concurrent")
    val executor = Executors.newFixedThreadPool(8)
    try {
      val clusterId = manager.getClusterIdByName("ck_cluster")
      val infoTemplate = DataInfoRecord(
        dbName = "pista_test",
        tbName = table,
        rDate = "",
        clusterId = clusterId,
        indexSize = 3)
      val infoRecords = runConcurrently(executor, 8) {
        manager.verifyDataInfoRecord(infoTemplate, overwrite = false).getOrElse(
          fail("failed to create concurrent DataInfoRecord"))
      }
      assert(infoRecords.map(_.id).distinct.size == 1)

      val shardTemplate = DataRecord(
        dbName = "pista_test",
        tbName = table,
        rDate = "",
        clusterId = clusterId,
        dataIndex = 0,
        indexSize = 3,
        hostAddress = "127.0.0.1:8123")
      val shardRecords = runConcurrently(executor, 8) {
        manager.verifyDataRecord(shardTemplate, overwrite = false).getOrElse(
          fail("Failed to create concurrent DataRecord"))
      }
      assert(shardRecords.map(_.id).distinct.size == 1)

      val completedShard = shardRecords.head.copy(
        originalDataVolume = 11L,
        insertDataVolume = 11L,
        status = RecordStatus.SUCCESS_VALUE)
      assert(manager.updateDataVolume(completedShard))
      assert(manager.updateTaskStatus(completedShard))
      assert(manager.searchRecord(completedShard).exists { record =>
        record.status == RecordStatus.SUCCESS_VALUE && record.insertDataVolume == 11L
      })

      JdbcAssertUtils.withConnection(
        postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
        assert(JdbcAssertUtils.queryLong(
          connection,
          s"""SELECT count(*) FROM clickhouse_data_info
             |WHERE dbname = 'pista_test' AND tbname = '$table'
             |AND rdate = '' AND cluster_id = $clusterId""".stripMargin) == 1L)
        assert(JdbcAssertUtils.queryLong(
          connection,
          s"""SELECT count(*) FROM clickhouse_data_records
             |WHERE dbname = 'pista_test' AND tbname = '$table'
             |AND rdate = '' AND cluster_id = $clusterId
             |AND data_index = 0 AND index_size = 3""".stripMargin) == 1L)
      }
    } finally {
      executor.shutdownNow()
      manager.close()
    }
  }

  private def runConcurrently[A](
    executor: java.util.concurrent.ExecutorService,
    count: Int
  )(operation: => A): Seq[A] = {
    val ready = new CountDownLatch(count)
    val start = new CountDownLatch(1)
    val futures = (1 to count).map { _ =>
      executor.submit(new Callable[A] {
        override def call(): A = {
          ready.countDown()
          if (!start.await(30, TimeUnit.SECONDS))
            throw new IllegalStateException("Concurrent metadata task did not start within 30 seconds")
          operation
        }
      })
    }
    assert(ready.await(30, TimeUnit.SECONDS))
    start.countDown()
    futures.map(_.get(60, TimeUnit.SECONDS))
  }

  private def newConnection(): MetaConnection = {
    val postgres = ContainerSuite.postgres
    new MetaConnection(MetaConf(
      host = postgres.getHost,
      port = postgres.getMappedPort(5432),
      database = "pista_meta",
      username = postgres.metaUser,
      password = postgres.metaPassword))
  }
}
