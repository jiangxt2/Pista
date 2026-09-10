package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.{JdbcAssertUtils, MetaSchemaInitializer, TestDataGenerator}

class DorisMetaManagerIT extends BaseIT {

  test("DorisMetaManager covering running, success, failure, and idempotent upsert states upsert status") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
    val conf = DorisMetaConf(
      pgHost = postgres.getHost,
      pgPort = postgres.getMappedPort(5432),
      pgDatabase = "pista_meta",
      pgUsername = postgres.metaUser,
      pgPassword = postgres.metaPassword,
      clusterName = "pista_it")
    val manager = new DorisMetaManager(new DorisMetaConnection(conf), conf)
    val taskId = TestDataGenerator.uniqueName("pista_it_doris_meta")

    try {
      manager.upsertRunning(
        taskId,
        "pista_test",
        "meta_it_task",
        "20260801",
        "stream_load",
        Some("catalog.db.source"),
        Some("biz_date = '20260801'"))
      manager.upsertRunning(
        taskId,
        "pista_test",
        "meta_it_task",
        "20260801",
        "stream_load",
        Some("catalog.db.source"),
        Some("biz_date = '20260801'"))

      assert(manager.hasAnyRecord(taskId))
      assert(!manager.isSucceeded(taskId))
      assert(queryTaskCount(postgres, taskId) == 1L)
      assert(queryStatus(postgres, taskId) == DorisRecordStatus.RUNNING.id)

      manager.markSuccess(taskId, 17L)
      assert(manager.isSucceeded(taskId))
      JdbcAssertUtils.withConnection(
        postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
        JdbcAssertUtils.firstRow(
          connection,
          s"""SELECT status, written_rows, source_table, partition_filter, end_time
             |FROM doris_task_info WHERE task_id = '$taskId'""".stripMargin) { result =>
          assert(result.getInt("status") == DorisRecordStatus.SUCCESS.id)
          assert(result.getLong("written_rows") == 17L)
          assert(result.getString("source_table") == "catalog.db.source")
          assert(result.getString("partition_filter") == "biz_date = '20260801'")
          assert(result.getTimestamp("end_time") != null)
        }
      }

      manager.upsertRunning(
        taskId, "pista_test", "meta_it_task", "20260801",
        "stream_load", None, None)
      assert(queryTaskCount(postgres, taskId) == 1L)
      assert(queryStatus(postgres, taskId) == DorisRecordStatus.RUNNING.id)

      manager.markFailure(taskId, "injected failure")
      assert(queryStatus(postgres, taskId) == DorisRecordStatus.FAILURE.id)
      assert(!manager.isSucceeded(taskId))
    } finally manager.close()
  }

  private def queryTaskCount(
    postgres: com.pista.spark.sql.test.container.PistaPostgresContainer,
    taskId: String
  ): Long =
    JdbcAssertUtils.withConnection(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
      JdbcAssertUtils.queryLong(
        connection,
        s"SELECT count(*) FROM doris_task_info WHERE task_id = '$taskId'")
    }

  private def queryStatus(
    postgres: com.pista.spark.sql.test.container.PistaPostgresContainer,
    taskId: String
  ): Int =
    JdbcAssertUtils.withConnection(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
      JdbcAssertUtils.queryLong(
        connection,
        s"SELECT status FROM doris_task_info WHERE task_id = '$taskId'").toInt
    }
}
