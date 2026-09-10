package com.pista.spark.sql.test.util

import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite

import java.util.concurrent.{Callable, Executors, TimeUnit}

class MetaSchemaInitializerIT extends BaseIT {
  test("Meta schema concurrent initialization maintains idempotence and does not delete existing data") {
    val postgres = ContainerSuite.postgres
    MetaSchemaInitializer.initializeSchemaOnly(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)

    val sentinel = TestDataGenerator.uniqueName("meta_initializer_sentinel")
    JdbcAssertUtils.withConnection(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
      val insert = connection.prepareStatement(
        """INSERT INTO clickhouse_data_info
          |(dbname, tbname, rdate, cluster_id, index_size, data_volume, status)
          |SELECT 'pista_test', ?, '', id, 1, 7, 1
          |FROM clickhouse_cluster_info
          |WHERE cluster_name = 'ck_cluster'""".stripMargin)
      try {
        insert.setString(1, sentinel)
        assert(insert.executeUpdate() == 1)
      } finally insert.close()
    }

    val executor = Executors.newFixedThreadPool(4)
    try {
      val tasks = (1 to 8).map { _ =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit =
            MetaSchemaInitializer.initializeSchemaOnly(
              postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
        })
      }
      tasks.foreach(_.get(60, TimeUnit.SECONDS))
    } finally executor.shutdownNow()

    JdbcAssertUtils.withConnection(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword) { connection =>
      val sentinelCount = JdbcAssertUtils.queryLong(
        connection,
        s"SELECT count(*) FROM clickhouse_data_info WHERE tbname = '$sentinel'")
      val clusterCount = JdbcAssertUtils.queryLong(
        connection,
        "SELECT count(*) FROM clickhouse_cluster_info WHERE cluster_name = 'ck_cluster'")
      assert(sentinelCount == 1L)
      assert(clusterCount == 1L)

      val cleanup = connection.prepareStatement(
        "DELETE FROM clickhouse_data_info WHERE tbname = ?")
      try {
        cleanup.setString(1, sentinel)
        cleanup.executeUpdate()
      } finally cleanup.close()
    }
  }
}
