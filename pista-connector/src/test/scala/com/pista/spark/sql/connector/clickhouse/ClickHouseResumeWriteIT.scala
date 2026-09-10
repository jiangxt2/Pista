package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.clickhouse.meta.record.{DataInfoRecord, DataRecord, RecordStatus}

class ClickHouseResumeWriteIT extends ClickHouseITSupport {
  test("ClickHouse Resume skip successful ones shard overwrite failed shards and purge shard") {
    val (database, table) = prepareTable("id Int32, value String", "it_ck_resume")
    val ids = 1 to 12
    val successIndex = 0
    val failureIndex = 1
    val successHost = clickHouseNodes(0)
    val failureHost = clickHouseNodes(1)
    val successIds = ids.filter(expectedShard(_) == successIndex)

    ckContainer.executeOn(
      0,
      s"INSERT INTO $database.$table VALUES " +
        successIds.map(id => s"($id, 'seed-$id')").mkString(","))
    ckContainer.executeOn(
      1,
      s"INSERT INTO $database.$table VALUES (999, 'partial')")

    withMetaManager { manager =>
      val clusterId = manager.getClusterIdByName("ck_cluster")
      manager.verifyDataInfoRecord(DataInfoRecord(
        dbName = database,
        tbName = table,
        rDate = "",
        clusterId = clusterId,
        indexSize = 3), overwrite = false).getOrElse(
        fail("Failed to create ClickHouse data info record"))

      def seedRecord(index: Int,
                     host: String,
                     originalRows: Long,
                     insertedRows: Long,
                     status: Int): Unit = {
        val persisted = manager.verifyDataRecord(DataRecord(
          dbName = database,
          tbName = table,
          rDate = "",
          clusterId = clusterId,
          dataIndex = index,
          indexSize = 3,
          hostAddress = host), overwrite = false).getOrElse(
          fail(s"failed to create shard $index Meta record"))
        val updated = persisted.copy(
          originalDataVolume = originalRows,
          insertDataVolume = insertedRows,
          status = status)
        assert(manager.updateDataVolume(updated))
        assert(manager.updateTaskStatus(updated))
      }

      seedRecord(successIndex, successHost, successIds.size, successIds.size,
        RecordStatus.SUCCESS_VALUE)
      seedRecord(failureIndex, failureHost, 4L, 1L, RecordStatus.FAILURE_VALUE)
    }

    val config = writerConfig(database, table)
    write(sourceRows(ids.map(id => id -> s"value-$id")), config)

    val records = shardMetaRecords(database, table)
    assert(records.size == 3)
    assert(records.forall(_.status == RecordStatus.SUCCESS_VALUE))
    assert(records.map(_.insertDataVolume).sum == ids.size)
    assertExactShardRows(database, table, ids)

    val successNode = clickHouseNodes.indexOf(successHost)
    assert(ckContainer.queryLong(
      successNode,
      s"SELECT count() FROM $database.$table WHERE value LIKE 'seed-%'") == successIds.size)
    assert(ckContainer.queryLong(
      1, s"SELECT count() FROM $database.$table WHERE id = 999") == 0L)
  }
}
