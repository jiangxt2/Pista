package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.{DorisConcurrentContext, DorisConcurrentWriter}

class DorisPartitionWriteIT extends DorisITSupport {

  test("Doris overwrite target partition atomically while retaining other partitions") {
    val (database, table) = createPartitionedTable("it_doris_partition")
    seedPartitions(database, table)

    val timestamp = System.currentTimeMillis()
    val config = batchConfig(
      database,
      table,
      overwrite = true,
      partitionDate = "20260801",
      partitionColumn = "biz_date")
    val source = partitionSource(Seq(
      (10, 20260801, "new-10"),
      (11, 20260801, "new-11")))

    val result = new DorisConcurrentWriter(
      config,
      DorisConcurrentContext(clusterName = None, metaManager = None),
      source,
      timestamp).write()

    assert(result.success)
    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260801 ORDER BY id")(
      _.getInt(1)) == Seq(10, 11))
    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260802")(
      _.getInt(1)) == Seq(2))
    assert(temporaryPartitionNames(database, table).isEmpty)
  }

  test("Doris Partition Overwrite Write Failure Retains Formal Partition and Cleans Temporary Partition") {
    val (database, table) = createPartitionedTable("it_doris_partition_failure")
    seedPartitions(database, table)

    val timestamp = System.currentTimeMillis()
    val config = batchConfig(
      database,
      table,
      overwrite = true,
      partitionDate = "20260801",
      partitionColumn = "biz_date")
    val source = partitionSource(Seq((20, 20260801, "replacement")))

    intercept[IllegalStateException] {
      failingAfterPreparationWriter(config, source, timestamp).write()
    }

    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260801")(
      _.getInt(1)) == Seq(1))
    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260802")(
      _.getInt(1)) == Seq(2))
    assert(temporaryPartitionNames(database, table).isEmpty)
  }

  private def createPartitionedTable(prefix: String): (String, String) =
    createDorisTable(
      prefix,
      """(id INT, biz_date INT, value VARCHAR(128))
        |DUPLICATE KEY(id, biz_date)
        |PARTITION BY RANGE(biz_date) (
        |  PARTITION p20260801 VALUES [(20260801), (20260802)),
        |  PARTITION p20260802 VALUES [(20260802), (20260803))
        |)
        |DISTRIBUTED BY HASH(id) BUCKETS 2
        |PROPERTIES ("replication_num" = "2")""".stripMargin)

  private def seedPartitions(database: String, table: String): Unit =
    executeDoris(
      s"""INSERT INTO `$database`.`$table` (id, biz_date, value) VALUES
         |(1, 20260801, 'old-target'),
         |(2, 20260802, 'keep')""".stripMargin)
}
