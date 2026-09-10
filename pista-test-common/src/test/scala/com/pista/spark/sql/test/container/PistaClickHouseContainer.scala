package com.pista.spark.sql.test.container

import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.model.Ulimit
import com.pista.spark.sql.test.util.RetryUtils
import org.testcontainers.containers.{BindMode, GenericContainer, Network}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.net.{HttpURLConnection, Socket, SocketTimeoutException, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.sql.{Connection, DriverManager, ResultSet}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{Callable, Executors}
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

final case class ClickHouseMachineRecord(hostPort: String,
                                        shardNum: Int,
                                        replicaNum: Int = 1,
                                        onlyRole: Int = 0,
                                        isAlive: Int = 1)

/** Three ClickHouse data nodes: three shards, one replica, and a three-node Keeper quorum. */
final class PistaClickHouseContainer(network: Network, resourcePrefix: String)
    extends AutoCloseable {

  private val image = DockerImageName.parse("clickhouse/clickhouse-server:25.3.2.39")
  private val clusterName = "ck_cluster"
  private val user = "pista"
  private val password = "test-password"
  private val nodeNames = (1 to 3).map(shard => s"ck-s${shard}r1")
  private val configFileNames = Seq(
    "macros.xml", "listen.xml", "resources.xml", "cluster.xml", "keeper.xml")
  private val configDirectoryPermissions = PosixFilePermissions.fromString("rwxr-xr-x")
  private val configFilePermissions = PosixFilePermissions.fromString("rw-r--r--")
  private val configDirs = ListBuffer.empty[Path]
  private val aliases = nodeNames.map(name => s"$resourcePrefix-$name")

  private final class CkNode extends GenericContainer[CkNode](image)

  private var nodes: Seq[CkNode] = Seq.empty
  private var closed = false

  def nodeCount: Int = nodes.size
  def username: String = user
  def userPassword: String = password
  def nodeAddresses: Seq[String] = nodes.map(node => s"${node.getHost}:${node.getMappedPort(8123)}")
  def internalNodeHost(nodeIndex: Int): String = aliases(nodeIndex)
  def entryJdbcUrl: String = jdbcUrl(selectHealthyEntry())

  def machineRecords: Seq[ClickHouseMachineRecord] = nodes.zipWithIndex.map { case (node, index) =>
    ClickHouseMachineRecord(
      hostPort = s"${node.getHost}:${node.getMappedPort(8123)}",
      shardNum = index + 1)
  }

  def start(): Unit = {
    require(!closed, "ClickHouse container is already closed")
    nodes = nodeNames.map(buildNode)
    startNodesConcurrently()
    awaitKeeperQuorum()
    executeOnEntry(s"CREATE DATABASE IF NOT EXISTS pista_test ON CLUSTER $clusterName")
  }

  def executeOnEntry(sql: String): Unit = execute(selectHealthyEntry(), sql)

  def executeOn(nodeIndex: Int, sql: String): Unit = execute(nodes(nodeIndex), sql)

  def queryLong(sql: String): Long = withConnection(entryJdbcUrl) { connection =>
    queryLong(connection, sql)
  }

  def queryLong(nodeIndex: Int, sql: String): Long = withConnection(jdbcUrl(nodes(nodeIndex))) { connection =>
    queryLong(connection, sql)
  }

  private def queryLong(connection: Connection, sql: String): Long = {
    val statement = connection.createStatement()
    try {
      val result = statement.executeQuery(sql)
      try {
        require(result.next(), s"ClickHouse query returned no rows: $sql")
        result.getLong(1)
      } finally result.close()
    } finally statement.close()
  }

  override def close(): Unit = synchronized {
    if (closed) return
    closed = true
    var failure: Throwable = null
    nodes.reverse.foreach { node =>
      try node.stop()
      catch {
        case e: com.github.dockerjava.api.exception.DockerException
            if e.getHttpStatus == 304 || e.getHttpStatus == 404 =>
        case t: Throwable =>
          if (failure == null) failure = t else failure.addSuppressed(t)
      }
    }
    configDirs.reverse.foreach { path =>
      try deleteExactPath(path)
      catch {
        case t: Throwable =>
          if (failure == null) failure = t else failure.addSuppressed(t)
      }
    }
    if (failure != null) throw failure
  }

  def captureLogs(): Unit = {
    nodes.zipWithIndex.foreach { case (node, index) =>
      ContainerLogUtils.capture(node.getDockerClient, node.getContainerId,
        s"$resourcePrefix-clickhouse-${index + 1}")
    }
  }

  private def buildNode(nodeName: String): CkNode = {
    val configDir = renderConfigDirectory(nodeName)
    val node = new CkNode()
    node.withNetwork(network)
      .withNetworkAliases(alias(nodeName))
      .withLabel("pista.it.managed", "true")
      .withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
      .withCreateContainerCmdModifier(new Consumer[CreateContainerCmd] {
        override def accept(cmd: CreateContainerCmd): Unit = {
          cmd.withMemory(1L << 30)
            .withMemorySwap(1L << 30)
            .withUlimits(new Ulimit("nofile", 262144L, 262144L))
        }
      })
      .withExposedPorts(8123, 9000, 9181)
      .withEnv("CLICKHOUSE_DB", "pista_test")
      .withEnv("CLICKHOUSE_USER", user)
      .withEnv("CLICKHOUSE_PASSWORD", password)
      .withLogConsumer(ContainerLogUtils.streamToReport(s"$resourcePrefix-$nodeName"))
      // Individual test files are mounted below while preserving the image's
      // docker_related_config.xml behavior in the native config.d directory.
    // Keep the image-owned config.d directory and mount individual files. A
    // whole-directory bind inherits Files.createTempDirectory's 0700 mode,
    // which the ClickHouse user cannot traverse on Linux hosted runners.
    configFileNames.foreach { fileName =>
      node.withFileSystemBind(configDir.resolve(fileName).toString,
        s"/etc/clickhouse-server/config.d/$fileName", BindMode.READ_ONLY)
    }
    node.waitingFor(Wait.forHttp("/ping").forPort(8123)
        .forResponsePredicate(_.contains("Ok"))
        .withStartupTimeout(Duration.ofSeconds(120)))
    node
  }

  private def startNodesConcurrently(): Unit = {
    val executor = Executors.newFixedThreadPool(nodes.size)
    try {
      val tasks = nodes.zipWithIndex.map { case (node, index) =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            Thread.sleep(index * 5000L)
            node.start()
          }
        })
      }
      var failure: Throwable = null
      tasks.foreach { task =>
        try task.get()
        catch {
          case e: java.util.concurrent.ExecutionException =>
            val cause = Option(e.getCause).getOrElse(e)
            if (failure == null) failure = cause else failure.addSuppressed(cause)
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            if (failure == null) failure = e else failure.addSuppressed(e)
        }
      }
      if (failure != null) throw failure
    } finally executor.shutdownNow()
  }

  private def renderConfigDirectory(nodeName: String): Path = {
    val directory = Files.createTempDirectory(s"$resourcePrefix-$nodeName-")
    configDirs += directory
    val shard = nodeNames.indexOf(nodeName) + 1
    val serverId = shard
    val files = Map(
      "macros.xml" ->
        s"""<clickhouse><macros><cluster>$clusterName</cluster><shard>$shard</shard><replica>$nodeName</replica></macros></clickhouse>""",
      "listen.xml" ->
        "<clickhouse><listen_host>0.0.0.0</listen_host><listen_try>1</listen_try></clickhouse>",
      "resources.xml" ->
        """<clickhouse>
          |<logger><console>true</console></logger>
          |<max_thread_pool_size>256</max_thread_pool_size>
          |<background_buffer_flush_schedule_pool_size>4</background_buffer_flush_schedule_pool_size>
          |<background_pool_size>16</background_pool_size>
          |<background_move_pool_size>2</background_move_pool_size>
          |<background_fetches_pool_size>2</background_fetches_pool_size>
          |<background_common_pool_size>2</background_common_pool_size>
          |<background_schedule_pool_size>16</background_schedule_pool_size>
          |<background_message_broker_schedule_pool_size>4</background_message_broker_schedule_pool_size>
          |<background_distributed_schedule_pool_size>4</background_distributed_schedule_pool_size>
          |<uncompressed_cache_size>67108864</uncompressed_cache_size>
          |<mark_cache_size>67108864</mark_cache_size>
          |<index_mark_cache_size>33554432</index_mark_cache_size>
          |</clickhouse>""".stripMargin,
      "cluster.xml" -> renderClusterXml,
      "keeper.xml" -> renderKeeperXml(serverId))
    files.foreach { case (name, content) =>
      val path = directory.resolve(name)
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      Files.setPosixFilePermissions(path, configFilePermissions)
    }
    Files.setPosixFilePermissions(directory, configDirectoryPermissions)
    directory
  }

  private def renderClusterXml: String = {
    val shards = nodeNames.zipWithIndex.map { case (name, index) =>
      s"<shard><replica><host>${alias(name)}</host><port>9000</port></replica></shard>"
    }.mkString
    val keeperNodes = aliases.map(name =>
      s"<node><host>$name</host><port>9181</port></node>").mkString
    s"""<clickhouse><remote_servers><$clusterName>$shards</$clusterName></remote_servers><zookeeper>$keeperNodes</zookeeper></clickhouse>"""
  }

  private def renderKeeperXml(serverId: Int): String = {
    val raftServers = aliases.zipWithIndex.map { case (name, index) =>
      s"<server><id>${index + 1}</id><hostname>$name</hostname><port>9234</port></server>"
    }.mkString
    s"""<clickhouse><keeper_server>
       |<tcp_port>9181</tcp_port><listen_host>0.0.0.0</listen_host>
       |<server_id>$serverId</server_id>
       |<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
       |<snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
       |<four_letter_word_white_list>ruok,mntr</four_letter_word_white_list>
       |<coordination_settings><operation_timeout_ms>10000</operation_timeout_ms><session_timeout_ms>30000</session_timeout_ms><raft_logs_level>warning</raft_logs_level></coordination_settings>
       |<raft_configuration>$raftServers</raft_configuration>
       |</keeper_server></clickhouse>""".stripMargin
  }

  private def awaitKeeperQuorum(): Unit = RetryUtils.retry(60, 2.seconds) {
    val probes = nodes.map(node => keeperProbe(node.getHost, node.getMappedPort(9181)))
    require(probes.forall(_.ruok == "imok"), s"Keeper ruok failed: ${probes.mkString(",")}")
    require(probes.count(_.state == "leader") == 1, s"Keeper leaders: ${probes.mkString(",")}")
    require(probes.count(_.state == "follower") == 2, s"Keeper followers: ${probes.mkString(",")}")
  }

  private final case class KeeperProbe(ruok: String, state: String)

  private def keeperProbe(host: String, port: Int): KeeperProbe = {
    val ruok = sendFourLetterWord(host, port, "ruok")
    val monitor = sendFourLetterWord(host, port, "mntr")
    val state = monitor.linesIterator
      .find(_.trim.startsWith("zk_server_state"))
      .flatMap(_.trim.split("\\s+").lastOption)
      .getOrElse("unknown")
    KeeperProbe(ruok.trim, state)
  }

  private def sendFourLetterWord(host: String, port: Int, command: String): String = {
    val socket = new Socket(host, port)
    socket.setSoTimeout(2000)
    try {
      val output = socket.getOutputStream
      output.write(command.getBytes(StandardCharsets.US_ASCII))
      output.flush()
      val bytes = new Array[Byte](4096)
      val result = new StringBuilder
      try {
        var read = socket.getInputStream.read(bytes)
        while (read >= 0) {
          result.append(new String(bytes, 0, read, StandardCharsets.UTF_8))
          read = socket.getInputStream.read(bytes)
        }
      } catch { case _: SocketTimeoutException => () }
      result.toString
    } finally socket.close()
  }

  private def alias(nodeName: String): String = s"$resourcePrefix-$nodeName"

  private def selectHealthyEntry(): CkNode = {
    nodes.find(node => scala.util.Try(isHealthy(node)).getOrElse(false)).getOrElse {
      throw new IllegalStateException(
        s"No healthy ClickHouse entry node is available for run $resourcePrefix")
    }
  }

  private def isHealthy(node: CkNode): Boolean = {
    if (!node.isRunning) false
    else {
      val connection = new URL(
        s"http://${node.getHost}:${node.getMappedPort(8123)}/ping")
        .openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(1000)
      connection.setReadTimeout(1000)
      try {
        if (connection.getResponseCode != 200) false
        else {
          val source = scala.io.Source.fromInputStream(connection.getInputStream)
          try source.mkString.contains("Ok")
          finally source.close()
        }
      } finally connection.disconnect()
    }
  }

  private def jdbcUrl(node: CkNode): String =
    s"jdbc:clickhouse://${node.getHost}:${node.getMappedPort(8123)}/default?http_connection_provider=HTTP_URL_CONNECTION"

  private def withConnection[T](url: String)(f: Connection => T): T = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    val connection = DriverManager.getConnection(url, properties)
    try f(connection)
    finally connection.close()
  }

  private def execute(node: CkNode, sql: String): Unit = withConnection(jdbcUrl(node)) { connection =>
    val statement = connection.createStatement()
    try statement.execute(sql)
    finally statement.close()
  }

  private def deleteExactPath(path: Path): Unit = {
    if (!Files.exists(path)) return
    val stream = Files.walk(path)
    try stream.sorted(java.util.Comparator.reverseOrder[Path]())
      .forEach(path => Files.deleteIfExists(path))
    finally stream.close()
  }
}
