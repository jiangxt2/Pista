package com.pista.spark.sql.doris.meta

import com.pista.spark.errors.PistaDorisException
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.{JdbcAssertUtils, MetaSchemaInitializer, TestDataGenerator}

import java.sql.SQLException

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

  test("new schema stores complete multi-date selectors and long predicates") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
    val manager = managerFor("pista_meta")
    val taskId = TestDataGenerator.uniqueName("pista_it_long_selector")
    val dates = (1 to 28).map(day => f"202610$day%02d")
    val rdate = dates.mkString(",")
    val filter = dates.map(date => s"'$date'").mkString("biz_date IN (", ", ", ")")
    assert(rdate.length > 64 && filter.length > 256)
    try {
      manager.upsertRunning(taskId, "pista_test", "target", rdate, "spark_connector", None, Some(filter))
      manager.markSuccess(taskId, 28L)
      withDatabase("pista_meta") { connection =>
        JdbcAssertUtils.firstRow(connection,
          s"SELECT rdate, partition_filter, written_rows FROM doris_task_info WHERE task_id = '$taskId'") { result =>
          assert(result.getString("rdate") == rdate)
          assert(result.getString("partition_filter") == filter)
          assert(result.getLong("written_rows") == 28L)
        }
      }
      manager.upsertRunning(taskId, "pista_test", "target", rdate, "spark_connector", None, Some(filter))
      withDatabase("pista_meta") { connection =>
        assert(JdbcAssertUtils.queryLong(connection,
          s"SELECT written_rows FROM doris_task_info WHERE task_id = '$taskId'") == -1L)
      }
    } finally manager.close()
  }

  test("retry refreshes the complete task payload without changing its identity") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
    val manager = managerFor("pista_meta")
    val taskId = TestDataGenerator.uniqueName("pista_it_payload_retry")
    val dates = (1 to 8).map(day => f"202610$day%02d").mkString(",")
    val filter = dates.split(",").map(date => s"'$date'").mkString("biz_date IN (", ", ", ")")
    try {
      manager.upsertRunning(taskId, "pista_test", "target", "20261001", "spark_connector",
        Some("catalog.db.original"), Some("biz_date = '20261001'"))
      manager.markSuccess(taskId, 1L)
      val id = withDatabase("pista_meta") { connection =>
        JdbcAssertUtils.queryLong(connection, s"SELECT id FROM doris_task_info WHERE task_id = '$taskId'")
      }
      manager.upsertRunning(taskId, "pista_test", "target", dates, "stream_load",
        Some("catalog.db.replacement"), Some(filter))
      withDatabase("pista_meta") { connection =>
        JdbcAssertUtils.firstRow(connection,
          s"SELECT * FROM doris_task_info WHERE task_id = '$taskId'") { result =>
          assert(result.getLong("id") == id)
          assert(result.getString("rdate") == dates)
          assert(result.getString("write_mode") == "stream_load")
          assert(result.getString("source_table") == "catalog.db.replacement")
          assert(result.getString("partition_filter") == filter)
          assert(result.getInt("status") == DorisRecordStatus.RUNNING.id)
          assert(result.getLong("written_rows") == -1L)
          assert(result.getTimestamp("end_time") == null)
        }
      }
      manager.upsertRunning(taskId, "pista_test", "target", "", "spark_connector", None, None)
      withDatabase("pista_meta") { connection =>
        JdbcAssertUtils.firstRow(connection,
          s"SELECT rdate, source_table, partition_filter FROM doris_task_info WHERE task_id = '$taskId'") { result =>
          assert(result.getString("rdate").isEmpty)
          assert(result.getString("source_table").isEmpty)
          assert(result.getString("partition_filter") == null)
        }
      }
      assert(queryTaskCount(postgres, taskId) == 1L)
    } finally manager.close()
  }

  test("legacy schema fails before insertion and migration preserves rows and coordination objects") {
    val database = createDatabase()
    withDatabase(database) { connection =>
      JdbcAssertUtils.execute(connection, resource("00_create_common_functions.sql"))
      JdbcAssertUtils.execute(connection, resource("07_create_doris_task_info.sql")
        .replace("rdate TEXT", "rdate VARCHAR(64)")
        .replace("partition_filter TEXT", "partition_filter VARCHAR(256)"))
      JdbcAssertUtils.execute(connection, resource("08_create_doris_partition_records.sql"))
    }
    val manager = managerFor(database)
    val retained = TestDataGenerator.uniqueName("pista_it_retained")
    val next = TestDataGenerator.uniqueName("pista_it_eight_dates")
    val dates = (1 to 8).map(day => f"202610$day%02d").mkString(",")
    try {
      manager.upsertRunning(retained, "pista_test", "target", "20261001", "spark_connector", None, None)
      manager.markSuccess(retained, 17L)
      val failure = intercept[PistaDorisException] {
        manager.upsertRunning(next, "pista_test", "target", dates, "spark_connector", None, None)
      }
      assert(failure.getMessage.contains("limit=64"))
      assert(failure.getMessage.contains("requestedChars=71"))
      assert(failure.getMessage.contains("migrations/doris_task_metadata_text.sql"))
      assert(!manager.hasAnyRecord(next))
      withDatabase(database) { connection =>
        JdbcAssertUtils.execute(connection,
          s"INSERT INTO doris_partition_records(task_id, partition_id, label, status) VALUES ('$retained', 0, 'pista_it', 1)")
        def indexCount: Long = JdbcAssertUtils.queryLong(connection,
          "SELECT count(*) FROM pg_indexes WHERE tablename = 'doris_task_info'")
        def triggerCount: Long = JdbcAssertUtils.queryLong(connection,
          "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'doris_task_info'::regclass AND NOT tgisinternal")
        val indexes = indexCount
        val triggers = triggerCount
        JdbcAssertUtils.execute(connection, resource("migrations/doris_task_metadata_text.sql"))
        JdbcAssertUtils.execute(connection, resource("migrations/doris_task_metadata_text.sql"))
        assert(indexCount == indexes)
        assert(triggerCount == triggers)
        assert(JdbcAssertUtils.queryLong(connection,
          "SELECT count(*) FROM pg_constraint WHERE conname = 'fk_partition_task' AND convalidated") == 1L)
        assert(JdbcAssertUtils.queryLong(connection, "SELECT count(*) FROM doris_partition_records") == 1L)
        JdbcAssertUtils.firstRow(connection,
          s"SELECT status, written_rows, rdate FROM doris_task_info WHERE task_id = '$retained'") { result =>
          assert(result.getInt("status") == DorisRecordStatus.SUCCESS.id)
          assert(result.getLong("written_rows") == 17L)
          assert(result.getString("rdate") == "20261001")
        }
      }
      // The same manager observes the migrated schema rather than caching the old limits.
      manager.upsertRunning(next, "pista_test", "target", dates, "spark_connector", None, None)
      manager.markSuccess(next, 8L)
      assert(manager.isSucceeded(next))
    } finally manager.close()
  }

  test("metadata queries and zero-row transitions fail instead of reporting missing history or success") {
    val empty = managerFor(createDatabase())
    try {
      val successFailure = intercept[PistaDorisException](empty.isSucceeded("pista_it_missing"))
      val historyFailure = intercept[PistaDorisException](empty.hasAnyRecord("pista_it_missing"))
      assert(successFailure.getCause.asInstanceOf[SQLException].getSQLState == "42P01")
      assert(historyFailure.getCause.asInstanceOf[SQLException].getSQLState == "42P01")
    } finally empty.close()

    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
    val initialized = managerFor("pista_meta")
    try {
      val missing = TestDataGenerator.uniqueName("pista_it_no_task")
      assert(intercept[PistaDorisException](initialized.markSuccess(missing, 1L)).getMessage.contains("affected 0 rows"))
      assert(intercept[PistaDorisException](initialized.markFailure(missing, "private-value")).getMessage.contains("affected 0 rows"))
    } finally initialized.close()
  }

  private def resource(name: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"postgre/$name")
    require(stream != null, s"Missing migration test resource: $name")
    try scala.io.Source.fromInputStream(stream, "UTF-8").mkString
    finally stream.close()
  }

  private def withDatabase[T](database: String)(operation: java.sql.Connection => T): T = {
    val postgres = ContainerSuite.postgres
    val url = postgres.metaJdbcUrl.stripSuffix("/pista_meta") + s"/$database"
    JdbcAssertUtils.withConnection(url, postgres.metaUser, postgres.metaPassword)(operation)
  }

  private def createDatabase(): String = {
    val database = TestDataGenerator.uniqueName("pista_it_meta")
    withDatabase("pista_meta") { connection =>
      JdbcAssertUtils.execute(connection, s"CREATE DATABASE $database")
    }
    database
  }

  private def managerFor(database: String): DorisMetaManager = {
    val postgres = ContainerSuite.postgres
    val conf = DorisMetaConf(postgres.getHost, postgres.getMappedPort(5432), database,
      postgres.metaUser, postgres.metaPassword, "pista_it")
    new DorisMetaManager(new DorisMetaConnection(conf), conf)
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
