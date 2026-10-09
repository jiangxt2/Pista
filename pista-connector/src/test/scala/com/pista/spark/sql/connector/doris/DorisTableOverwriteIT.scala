package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.{DorisConcurrentContext, DorisConcurrentWriter}
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.execution.datasources.writer.{OutputConfig, WriterRegistry}
import com.pista.spark.sql.test.util.{JdbcAssertUtils, TestDataGenerator}

class DorisTableOverwriteIT extends DorisITSupport {

  test("Doris Full Table Overwrite Using an Atomic Replacement of the Old Data") {
    val (database, table) = createTable("it_doris_table_overwrite")
    executeDoris(
      s"INSERT INTO `$database`.`$table` VALUES (1, 'old-1'), (2, 'old-2')")

    val timestamp = System.currentTimeMillis()
    val config = batchConfig(database, table, overwrite = true)
    withMetadataContext() { context =>
      val result = new DorisConcurrentWriter(config, context, twoColumnSource(10 to 12, "new"), timestamp).write()
      assert(result.success)
    }
    assertTaskState(config, DorisRecordStatus.SUCCESS.id, 3L)
    assert(queryColumn(s"SELECT id FROM `$database`.`$table` ORDER BY id")(
      _.getInt(1)) == Seq(10, 11, 12))
    assert(!tableExists(database, s"tmp_${table}_$timestamp"))
  }

  test("Doris Whole-table overwrite write fails, retaining the original table and cleaning up the temporary table") {
    val (database, table) = createTable("it_doris_table_failure")
    executeDoris(
      s"INSERT INTO `$database`.`$table` VALUES (1, 'original')")

    val timestamp = System.currentTimeMillis()
    val config = batchConfig(database, table, overwrite = true)
    val error = withMetadataContext() { context =>
      intercept[IllegalStateException] {
        failingAfterPreparationWriter(config, twoColumnSource(20 to 21), timestamp, context).write()
      }
    }

    assert(error.getMessage == "injected connector failure")
    assert(queryColumn(s"SELECT value FROM `$database`.`$table`")(
      _.getString(1)) == Seq("original"))
    assert(!tableExists(database, s"tmp_${table}_$timestamp"))
    assertTaskState(config, DorisRecordStatus.FAILURE.id, -1L)
  }

  test("public generic overwrite uses session metadata and replaces small forced-JDBC output atomically",
    new org.scalatest.Tag("com.pista.DorisOverwriteBoundaryIT")) {
    val (database, table) = createTable("it_doris_public_overwrite")
    executeDoris(s"INSERT INTO `$database`.`$table` VALUES (1, 'original')")
    val config = batchConfig(database, table, overwrite = false)
    withWriterConfiguration(config) {
      WriterRegistry.write(twoColumnSource(10 to 12), OutputConfig(
        Some(s"$database.$table"), "doris", "overwrite", Nil, Map("force.jdbc" -> "true")))
    }
    assert(queryColumn(s"SELECT id FROM `$database`.`$table` ORDER BY id")(_.getInt(1)) == Seq(10, 11, 12))
    assertTaskState(config, DorisRecordStatus.SUCCESS.id, 3L)
    assert(!queryColumn(s"SHOW TABLES FROM `$database`")(_.getString(1)).exists(_.startsWith(s"tmp_${table}_")))
  }

  test("long whole-table identities are bounded and recorded during a real Doris overwrite") {
    val database = TestDataGenerator.uniqueName("pista_long_database_" * 4)
    val table = TestDataGenerator.uniqueName("overwrite")
    executeDoris(s"CREATE DATABASE `$database`")
    executeDoris(s"""CREATE TABLE `$database`.`$table` (id INT, value VARCHAR(128))
      |DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 2
      |PROPERTIES ("replication_num" = "2")""".stripMargin)
    executeDoris(s"INSERT INTO `$database`.`$table` VALUES (1, 'original')")
    val cluster = "c" * 128
    val label = TestDataGenerator.uniqueName("pista_long_label_" * 2)
    val config = batchConfig(database, table, labelPrefix = label, overwrite = true)
    assert(s"${cluster}_${label}_${database}_${table}_all".length > 256)
    assert(expectedTaskId(config, cluster).matches("pista_sha256_[0-9a-f]{64}"))
    withMetadataContext(cluster) { context =>
      assert(new DorisConcurrentWriter(config, context, twoColumnSource(20 to 21), System.currentTimeMillis()).write().success)
    }
    assert(queryColumn(s"SELECT id FROM `$database`.`$table` ORDER BY id")(_.getInt(1)) == Seq(20, 21))
    assertTaskState(config, DorisRecordStatus.SUCCESS.id, 2L, cluster)
  }

  test("public overwrite rejects conflicting targets before metadata or either table changes",
    new org.scalatest.Tag("com.pista.DorisOverwriteBoundaryIT")) {
    val (database, requestedTable) = createTable("it_doris_requested")
    val (_, configuredTable) = createTable("it_doris_configured")
    executeDoris(s"INSERT INTO `$database`.`$requestedTable` VALUES (1, 'requested-original')")
    executeDoris(s"INSERT INTO `$database`.`$configuredTable` VALUES (2, 'configured-original')")
    val config = batchConfig(database, configuredTable)
    val failure = withWriterConfiguration(config) {
      intercept[RuntimeException] {
        WriterRegistry.write(twoColumnSource(10 to 12), OutputConfig(
          Some(s"$database.$requestedTable"), "doris", "overwrite", Nil, Map("force.jdbc" -> "true")))
      }
    }
    assert(failure.getMessage.contains("PistaDorisException"))
    assert(!failure.getMessage.contains(requestedTable))
    assert(!failure.getMessage.contains(configuredTable))
    assert(queryColumn(s"SELECT value FROM `$database`.`$requestedTable`")(_.getString(1)) == Seq("requested-original"))
    assert(queryColumn(s"SELECT value FROM `$database`.`$configuredTable`")(_.getString(1)) == Seq("configured-original"))
    val names = queryColumn(s"SHOW TABLES FROM `$database`")(_.getString(1))
    assert(!names.exists(name => name.startsWith(s"tmp_${requestedTable}_") || name.startsWith(s"tmp_${configuredTable}_")))
    JdbcAssertUtils.withConnection(metaJdbcUrl, metaUser, metaPassword) { connection =>
      val statement = connection.prepareStatement("SELECT count(*) FROM doris_task_info WHERE task_id = ?")
      try {
        statement.setString(1, expectedTaskId(config))
        val result = statement.executeQuery()
        try { assert(result.next() && result.getLong(1) == 0L) }
        finally result.close()
      } finally statement.close()
    }
  }

  private def createTable(prefix: String): (String, String) =
    createDorisTable(
      prefix,
      """(id INT, value VARCHAR(128))
        |DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 2
        |PROPERTIES ("replication_num" = "2")""".stripMargin)
}
