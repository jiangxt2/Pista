package com.pista.spark.sql.test.container

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration

final class PistaPostgresContainer(network: org.testcontainers.containers.Network,
                                   resourcePrefix: String)
  extends GenericContainer[PistaPostgresContainer](DockerImageName.parse("postgres:16.14"))
    with AutoCloseable {

  private val port = 5432
  private val database = "pista_meta"
  private val user = "pista"
  private val password = "test-password"

  withNetwork(network)
  withNetworkAliases(s"$resourcePrefix-postgres")
  withLabel("pista.it.managed", "true")
  withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
  withEnv("POSTGRES_DB", database)
  withEnv("POSTGRES_USER", user)
  withEnv("POSTGRES_PASSWORD", password)
  withExposedPorts(port)
  withLogConsumer(ContainerLogUtils.streamToReport(s"$resourcePrefix-postgres"))
  // PostgreSQL opens its TCP port before startup completes. Probe the final
  // TCP server, not the temporary Unix-socket-only initialization server.
  // Keep the password in the container environment rather than the command.
  waitingFor(Wait.forSuccessfulCommand(
    "PGCONNECT_TIMEOUT=2 PGPASSWORD=\"$POSTGRES_PASSWORD\" " +
      s"psql --no-psqlrc --host=127.0.0.1 --port=$port --username=$user " +
      s"--dbname=$database --no-password --set=ON_ERROR_STOP=1 " +
      "--tuples-only --command='SELECT 1' >/dev/null")
    .withStartupTimeout(Duration.ofSeconds(60)))

  def metaJdbcUrl: String = s"jdbc:postgresql://${getHost}:${getMappedPort(port)}/$database"
  def metaUser: String = user
  def metaPassword: String = password

  def captureLogs(): Unit =
    ContainerLogUtils.capture(getDockerClient, getContainerId, s"$resourcePrefix-postgres")
}
