package com.pista.spark.sql.test.container

import com.pista.spark.sql.test.base.BaseIT
import org.testcontainers.containers.ContainerLaunchException

import java.nio.file.Files
import java.sql.DriverManager
import java.time.Duration
import java.util.UUID

class PistaPostgresContainerIT extends BaseIT {
  test("PostgreSQL start returns only when the first authenticated JDBC query can succeed") {
    val prefix = resourcePrefix()
    val postgres = new PistaPostgresContainer(ContainerSuite.network, prefix)
    try {
      postgres.start()
      val connection = DriverManager.getConnection(
        postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword)
      try {
        val statement = connection.createStatement()
        try {
          val result = statement.executeQuery("SELECT current_database(), current_user, 1")
          try {
            assert(result.next())
            assert(result.getString(1) == "pista_meta")
            assert(result.getString(2) == postgres.metaUser)
            assert(result.getInt(3) == 1)
          } finally result.close()
        } finally statement.close()
      } finally connection.close()
    } finally {
      try postgres.captureLogs()
      finally postgres.close()
    }
  }

  test("an open PostgreSQL port does not satisfy readiness when the target database is unavailable") {
    val prefix = resourcePrefix()
    val postgres = new PistaPostgresContainer(ContainerSuite.network, prefix)
    postgres.withEnv("POSTGRES_DB", "pista_other_database")
    postgres.withStartupTimeout(Duration.ofSeconds(15))
    postgres.withStartupAttempts(1)
    try {
      intercept[ContainerLaunchException] {
        postgres.start()
      }
      val log = Files.readString(ContainerLogUtils.reportPath(s"$prefix-postgres"))
      assert(log.contains("listening on IPv4 address"))
      assert(log.contains("port 5432"))
      assert(log.contains("database \"pista_meta\" does not exist"))
    } finally postgres.close()
  }

  private def resourcePrefix(): String =
    s"pista-it-${ContainerSuite.currentRunId}-${UUID.randomUUID().toString.take(8)}"
}
