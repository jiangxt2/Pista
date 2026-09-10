package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.connector.doris.batch.{SparkDorisConnectorWriter, WriteMode}

class DorisStreamLoadIT extends DorisITSupport {
  test("Doris Spark connector distributes data through Stream Load to two BE") {
    val (database, table) = createDorisTable(
      "it_doris_stream",
      """(id BIGINT, value VARCHAR(128))
        |DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 4
        |PROPERTIES ("replication_num" = "2")""".stripMargin)

    val source = spark.range(100001).selectExpr("id", "concat('value-', id) AS value")
    val config = batchConfig(database, table, writeMode = WriteMode.STREAM_LOAD)
    val result = new SparkDorisConnectorWriter(config, source, System.currentTimeMillis()).write()
    assert(result.success)

    assert(queryLong(s"SELECT COUNT(*) FROM `$database`.`$table`") == 100001L)

    val tabletBackendIds = queryColumn(
      s"SHOW TABLETS FROM `$database`.`$table`")(_.getLong("BackendId")).toSet
    val aliveBackendIds = queryColumn("SHOW BACKENDS") { result =>
      if (result.getBoolean("Alive")) Some(result.getLong("BackendId")) else None
    }.flatten.toSet

    assert(tabletBackendIds.size == 2,
      s"replicas should be distributed to two BEactual BackendIds are distributed to two BEs BackendId=$tabletBackendIds")
    assert(tabletBackendIds.subsetOf(aliveBackendIds))
  }
}
