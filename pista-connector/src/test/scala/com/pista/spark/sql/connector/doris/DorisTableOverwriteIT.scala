package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.{DorisConcurrentContext, DorisConcurrentWriter}

class DorisTableOverwriteIT extends DorisITSupport {

  test("Doris Full Table Overwrite Using an Atomic Replacement of the Old Data") {
    val (database, table) = createTable("it_doris_table_overwrite")
    executeDoris(
      s"INSERT INTO `$database`.`$table` VALUES (1, 'old-1'), (2, 'old-2')")

    val timestamp = System.currentTimeMillis()
    val config = batchConfig(database, table, overwrite = true)
    val result = new DorisConcurrentWriter(
      config,
      DorisConcurrentContext(clusterName = None, metaManager = None),
      twoColumnSource(10 to 12, "new"),
      timestamp).write()

    assert(result.success)
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
    val error = intercept[IllegalStateException] {
      failingAfterPreparationWriter(
        config,
        twoColumnSource(20 to 21),
        timestamp).write()
    }

    assert(error.getMessage == "injected connector failure")
    assert(queryColumn(s"SELECT value FROM `$database`.`$table`")(
      _.getString(1)) == Seq("original"))
    assert(!tableExists(database, s"tmp_${table}_$timestamp"))
  }

  private def createTable(prefix: String): (String, String) =
    createDorisTable(
      prefix,
      """(id INT, value VARCHAR(128))
        |DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 2
        |PROPERTIES ("replication_num" = "2")""".stripMargin)
}
