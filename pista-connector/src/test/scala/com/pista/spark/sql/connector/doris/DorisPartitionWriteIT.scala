package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.{DorisConcurrentContext, DorisConcurrentWriter}
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}

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

    withMetadataContext() { context =>
      assert(new DorisConcurrentWriter(config, context, source, timestamp).write().success)
    }
    assertTaskState(config, DorisRecordStatus.SUCCESS.id, 2L)
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

    withMetadataContext() { context =>
      intercept[IllegalStateException] {
        failingAfterPreparationWriter(config, source, timestamp, context).write()
      }
    }

    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260801")(
      _.getInt(1)) == Seq(1))
    assert(queryColumn(
      s"SELECT id FROM `$database`.`$table` WHERE biz_date = 20260802")(
      _.getInt(1)) == Seq(2))
    assert(temporaryPartitionNames(database, table).isEmpty)
    assertTaskState(config, DorisRecordStatus.FAILURE.id, -1L)
  }

  test("public overwrite replaces eight partitions and persists their complete metadata",
    new org.scalatest.Tag("com.pista.DorisOverwriteBoundaryIT")) {
    val dates = (1 to 9).map(day => 20261000 + day)
    val definitions = dates.map(date => s"PARTITION p$date VALUES [($date), (${date + 1}))").mkString(",\n")
    val (database, table) = createDorisTable("it_doris_many_partitions",
      s"""(id INT, biz_date INT, value VARCHAR(128))
         |DUPLICATE KEY(id, biz_date)
         |PARTITION BY RANGE(biz_date) ($definitions)
         |DISTRIBUTED BY HASH(id) BUCKETS 2
         |PROPERTIES ("replication_num" = "2")""".stripMargin)
    val old = dates.map(date => s"(1, $date, 'original')").mkString(",")
    executeDoris(s"INSERT INTO `$database`.`$table` VALUES $old")
    val selected = dates.take(8)
    val config = batchConfig(database, table, overwrite = false,
      partitionDate = selected.mkString(","), partitionColumn = "biz_date")
    assert(config.partitionDate.length == 71)
    withWriterConfiguration(config) {
      WriterRegistry.write(partitionSource(selected.map(date => (10, date, "replacement"))),
        OutputConfig(Some(s"$database.$table"), "doris", "overwrite", Nil, Map.empty))
    }
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table` WHERE id = 10") == 8L)
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table` WHERE id = 1 AND biz_date = ${dates.last}") == 1L)
    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 9L)
    assert(temporaryPartitionNames(database, table).isEmpty)
    assertTaskState(config, DorisRecordStatus.SUCCESS.id, 8L)
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
