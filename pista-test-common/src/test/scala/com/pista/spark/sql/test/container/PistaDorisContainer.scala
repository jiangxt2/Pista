package com.pista.spark.sql.test.container

import com.github.dockerjava.api.model.Ulimit
import org.testcontainers.containers.{GenericContainer, Network}
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.sql.DriverManager
import java.time.Duration
import scala.collection.JavaConverters._
import scala.util.Try

/** Doris 1 FE + 2 BE wrapper using the official ELECTION entrypoint contract. */
final class PistaDorisContainer(network: Network, resourcePrefix: String)
    extends AutoCloseable {
  private val version = sys.props.getOrElse("doris.version", "3.0.6.2")
  private val feImage = DockerImageName.parse(s"apache/doris:fe-$version")
  private val beImage = DockerImageName.parse(s"apache/doris:be-$version")
  private val feAlias = s"$resourcePrefix-doris-fe"
  private val beAliases = Seq("doris-be-1", "doris-be-2").map(alias => s"$resourcePrefix-$alias")

  private final class DorisNode(image: DockerImageName)
      extends GenericContainer[DorisNode](image)

  private val fe = new DorisNode(feImage)
  private var bes: Seq[DorisNode] = Seq.empty
  private var closed = false

  fe.withNetwork(network)
    .withNetworkAliases(feAlias)
    .withLabel("pista.it.managed", "true")
    .withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
    .withCreateContainerCmdModifier(cmd =>
      cmd.withMemory(2L << 30).withMemorySwap(2L << 30)
        .withUlimits(new Ulimit("nofile", 262144L, 262144L))
        .withEntrypoint("bash", "/usr/local/bin/pista-fe-entrypoint.sh"))
    .withCopyFileToContainer(MountableFile.forClasspathResource("doris/fe-entrypoint.sh"),
      "/usr/local/bin/pista-fe-entrypoint.sh")
    .withLogConsumer(ContainerLogUtils.streamToReport(s"$resourcePrefix-doris-fe"))
    .withEnv("PISTA_FE_ID", "1")
    .withExposedPorts(8030, 9030)
    .waitingFor(new HttpWaitStrategy()
      .forPort(8030)
      .forPath("/api/bootstrap")
      .forResponsePredicate(_.contains("\"code\":0"))
      .withStartupTimeout(Duration.ofSeconds(300)))

  def start(): Unit = {
    require(!closed, "Doris container is already closed")
    fe.start()
    val feIp = containerNetworkIp(fe)
    bes = beAliases.map(alias => buildBe(alias, feIp))
    // A private hosted runner has two CPUs; serial startup avoids competing
    // Doris initialization spikes while preserving the two-BE test topology.
    bes.foreach(_.start())
    waitBackendsAlive()
  }

  def feHost: String = fe.getHost
  def internalFeEndpoint: String = s"$feAlias:8030"
  def feQueryPort: Int = fe.getMappedPort(9030)
  def feHttpPort: Int = fe.getMappedPort(8030)
  def feHttpEndpoint: String = s"$feHost:$feHttpPort"
  def mysqlJdbcUrl: String = s"jdbc:mysql://$feHost:$feQueryPort/information_schema"
  def beHttpEndpoints: Seq[String] = bes.map(node => s"${node.getHost}:${node.getMappedPort(8040)}")

  override def close(): Unit = synchronized {
    if (closed) return
    closed = true
    var failure: Throwable = null
    (bes.reverse :+ fe).foreach { container =>
      try container.stop()
      catch {
        case e: com.github.dockerjava.api.exception.DockerException
            if e.getHttpStatus == 304 || e.getHttpStatus == 404 =>
        case t: Throwable =>
          if (failure == null) failure = t else failure.addSuppressed(t)
      }
    }
    if (failure != null) throw failure
  }

  private def buildBe(alias: String, feIp: String): DorisNode = {
    new DorisNode(beImage)
      .withNetwork(network)
      .withNetworkAliases(alias)
      .withLabel("pista.it.managed", "true")
      .withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
      .withCreateContainerCmdModifier(cmd =>
        cmd.withMemory(2L << 30).withMemorySwap(2L << 30)
          .withUlimits(new Ulimit("nofile", 262144L, 262144L))
          .withEntrypoint("bash", "/usr/local/bin/pista-be-entrypoint.sh"))
      .withCopyFileToContainer(MountableFile.forClasspathResource("doris/be-entrypoint.sh"),
        "/usr/local/bin/pista-be-entrypoint.sh")
      .withLogConsumer(ContainerLogUtils.streamToReport(alias))
      .withEnv("PISTA_FE_IP", feIp)
      .withEnv("PISTA_FE_EDIT_LOG_PORT", "9010")
      .withExposedPorts(8040, 9050)
      .waitingFor(new HttpWaitStrategy()
        .forPort(8040)
        .forPath("/api/health")
        .withStartupTimeout(Duration.ofSeconds(300)))
  }

  private def waitBackendsAlive(): Unit = {
    val expectedIps = bes.map(containerNetworkIp).toSet
    var attempt = 0
    var alive = false
    while (!alive && attempt < 90) {
      Thread.sleep(2000)
      alive = scala.util.Try {
        val connection = DriverManager.getConnection(mysqlJdbcUrl, "root", "")
        try {
          val result = connection.createStatement().executeQuery("SHOW BACKENDS")
          val hosts = scala.collection.mutable.Set.empty[String]
          try {
            while (result.next()) {
              if (result.getString("Alive") == "true" &&
                  result.getInt("HeartbeatPort") == 9050 &&
                  PistaDorisContainer.hasPositiveCapacity(result.getString("TotalCapacity")) &&
                  PistaDorisContainer.hasPositiveCapacity(result.getString("AvailCapacity")))
                hosts += result.getString("Host")
            }
          } finally result.close()
          hosts == expectedIps
        } finally connection.close()
      }.getOrElse(false)
      attempt += 1
    }
    if (!alive)
      throw new IllegalStateException(
        s"Doris BE ${expectedIps.mkString(",")} failed to report alive storage within 180s")
  }

  private def containerNetworkIp(container: DorisNode): String = {
    val info = container.getDockerClient
      .inspectContainerCmd(container.getContainerId).exec()
    val networks = Option(info.getNetworkSettings).toSeq
      .flatMap(settings => Option(settings.getNetworks).toSeq.flatMap(_.asScala.values))
    networks.find(_.getNetworkID == network.getId)
      .flatMap(value => Option(value.getIpAddress))
      .getOrElse(throw new IllegalStateException(
        s"Container ${container.getContainerId} is not attached to network ${network.getId}"))
  }

}

private[container] object PistaDorisContainer {
  def hasPositiveCapacity(value: String): Boolean =
    Option(value).map(_.trim).filter(_.nonEmpty)
      .flatMap(_.split("\\s+", 2).headOption)
      .flatMap(number => Try(BigDecimal(number)).toOption)
      .exists(_ > 0)
}
