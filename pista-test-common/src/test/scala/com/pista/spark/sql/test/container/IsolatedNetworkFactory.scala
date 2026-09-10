package com.pista.spark.sql.test.container

import com.github.dockerjava.api.command.CreateNetworkCmd
import com.github.dockerjava.api.model.{Network => DockerNetwork}
import com.pista.spark.sql.test.util.CidrUtils
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.Network

import java.util.UUID
import java.util.function.Consumer
import scala.collection.JavaConverters._

/** Creates one dynamically addressed network and never touches pre-existing networks. */
object IsolatedNetworkFactory {
  private val MaxAttempts = 10

  private def allIpv4IpamSubnets(network: DockerNetwork): Seq[String] =
    Option(network.getIpam).toSeq
      .flatMap(ipam => Option(ipam.getConfig).toSeq.flatMap(_.asScala))
      .flatMap(config => Option(config.getSubnet))
      .filter(CidrUtils.isIpv4Cidr)

  def create(prefix: String, runId: String): Network = {
    val docker = DockerClientFactory.instance().client()
    var attempt = 0
    var lastFailure: Throwable = null

    while (attempt < MaxAttempts) {
      val usedSubnets = docker.listNetworksCmd().exec().asScala.flatMap(allIpv4IpamSubnets).toSeq
      val subnet = CidrUtils.nextNonOverlappingPrivate24(usedSubnets, attempt)
      val name = s"${safePrefix(prefix)}-net-${UUID.randomUUID().toString.replace("-", "").take(8)}"
      val candidate = Network.builder()
        .createNetworkCmdModifier(new Consumer[CreateNetworkCmd] {
          override def accept(cmd: CreateNetworkCmd): Unit = {
            val ipam = new DockerNetwork.Ipam()
              .withConfig(new DockerNetwork.Ipam.Config().withSubnet(subnet))
            val labels = new java.util.HashMap[String, String]()
            labels.put("pista.it.managed", "true")
            labels.put("pista.it.run-id", runId)
            cmd.withName(name).withLabels(labels).withIpam(ipam)
          }
        })
        .build()

      try {
        val candidateId = candidate.getId
        val actual = docker.inspectNetworkCmd().withNetworkId(candidateId).exec()
        val actualSubnets = allIpv4IpamSubnets(actual)
        val concurrentSubnets = docker.listNetworksCmd().exec().asScala
          .filter(_.getId != candidateId)
          .flatMap(allIpv4IpamSubnets)
        val overlapsExisting = actualSubnets.exists(actualSubnet =>
          (usedSubnets ++ concurrentSubnets).exists(CidrUtils.overlaps(actualSubnet, _)))

        if (actualSubnets.contains(subnet) && !overlapsExisting) return candidate
        candidate.close()
      } catch {
        case t: Throwable =>
          lastFailure = t
          try candidate.close() catch { case _: Throwable => () }
      }
      attempt += 1
    }

    throw new IllegalStateException(
      s"Unable to create isolated network after $MaxAttempts attempts in 172.16.0.0/12",
      lastFailure)
  }

  private def safePrefix(prefix: String): String = {
    val value = prefix.replaceAll("[^A-Za-z0-9_.-]", "-")
    if (value.nonEmpty) value.take(48) else "pista-it"
  }
}
