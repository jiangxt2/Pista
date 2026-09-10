package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.clickhouse.meta.record.RecordStatus

/** End-to-end ClickHouse key-sharding test backed by the three-node test cluster. */
class ClickHouseKeyBatchWriteIT extends ClickHouseITSupport {

  test("ClickHouse writer routes fixed primary keys precisely to three shards allocated by Meta") {
    val (database, table) = uniqueClickHouseTable("it_ck_key")
    val tablePath = s"$database.$table"
    ckContainer.executeOnEntry(
      s"""CREATE TABLE $tablePath ON CLUSTER ck_cluster
         |(id Int32, value String)
         |ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$table', '{replica}')
         |ORDER BY id""".stripMargin)

    val ids = 1 to 30
    write(sourceRows(ids.map(index => index -> s"value-$index")), writerConfig(database, table))

    val records = shardMetaRecords(database, table)
    assert(records.size == 3)
    assert(records.forall(_.status == RecordStatus.SUCCESS_VALUE))
    assert(records.map(_.insertDataVolume).sum == ids.size)
    assertExactShardRows(database, table, ids)
  }
}
