package com.pista.spark.sql.test.container

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.nio.file.Paths
import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit, TimeoutException}
import scala.collection.JavaConverters._
import org.testcontainers.containers.Container

/** Spark 3.5.8 runtime used by Submitter end-to-end IT. */
final class PistaSparkContainer(network: org.testcontainers.containers.Network,
                                resourcePrefix: String)
  extends GenericContainer[PistaSparkContainer](DockerImageName.parse(
    sys.props.getOrElse("spark.image", "apache/spark:3.5.8")))
    with AutoCloseable {

  private val sparkSubmit = "/opt/spark/bin/spark-submit"

  withNetwork(network)
  withNetworkAliases(s"$resourcePrefix-spark")
  withLabel("pista.it.managed", "true")
  withLabel("pista.it.run-id", resourcePrefix.stripPrefix("pista-it-"))
  withCreateContainerCmdModifier(cmd =>
    // SparkContainerIT only runs --version; Submitter IT also starts a local
    // Spark driver and executes a write, so leave headroom for JVM/native memory.
    cmd.withMemory(3L << 30).withMemorySwap(3L << 30))
  withCommand("bash", "-lc", "tail -f /dev/null")

  private var closed = false

  /** Connect only to a network created by the current ContainerSuite. */
  def connectToNetwork(targetNetwork: org.testcontainers.containers.Network): Unit = {
    require(getContainerId != null, "Spark container must be started before connecting a network")
    require(targetNetwork != null && targetNetwork.getId != null, "target network must be created")
    getDockerClient.connectToNetworkCmd()
      .withContainerId(getContainerId)
      .withNetworkId(targetNetwork.getId)
      .exec()
  }

  /**
   * Copy the local assembly and SQL file into this run's container and invoke
   * the real SparkSQLSubmitter main class through spark-submit.
   */
  def submit(sqlFile: String, params: util.Map[String, String]): Unit =
    submit(sqlFile, params, util.Collections.emptyMap[String, String]())

  def submit(sqlFile: String,
             params: util.Map[String, String],
             submitterConf: util.Map[String, String]): Unit = {
    val result = submitResult(sqlFile, params, submitterConf)
    if (result.getExitCode != 0)
      throw new IllegalStateException(
        s"spark-submit failed with exit=${result.getExitCode}\nstdout=${result.getStdout}\nstderr=${result.getStderr}")
  }

  /** Execute SparkSQLSubmitter and return stdout, stderr, and the process exit code. */
  def submitResult(sqlFile: String,
                   params: util.Map[String, String],
                   submitterConf: util.Map[String, String]): Container.ExecResult = {
    val assembly = sys.props.get("pista.it.submitter.jar").filter(_.trim.nonEmpty).getOrElse(
      throw new IllegalStateException("pista.it.submitter.jar is required for Submitter IT"))
    val sqlPath = Paths.get(sqlFile)
    val jarPath = Paths.get(assembly)
    require(java.nio.file.Files.isRegularFile(sqlPath), s"SQL file does not exist: $sqlFile")
    require(java.nio.file.Files.isRegularFile(jarPath), s"Submitter JAR does not exist: $assembly")

    copyFileToContainer(MountableFile.forHostPath(jarPath), "/opt/pista-it/pista-batch.jar")
    // Files.createTempFile creates a 0600 file. The Spark image runs as the
    // spark user, so preserve the content but explicitly copy it as readable.
    copyFileToContainer(MountableFile.forHostPath(sqlPath, 420), "/opt/pista-it/query.sql")

    val requiresIceberg = submitterConf.asScala.keys.exists(_.startsWith("spark.sql.catalog."))
    val icebergJar = if (requiresIceberg) {
      val path = sys.props.get("pista.it.iceberg.jar").map(Paths.get(_)).getOrElse(
        throw new IllegalStateException("pista.it.iceberg.jar is required for Iceberg Submitter IT"))
      require(java.nio.file.Files.isRegularFile(path), s"Iceberg runtime JAR does not exist: $path")
      Some(path)
    } else None
    icebergJar.foreach(path =>
      copyFileToContainer(MountableFile.forHostPath(path), "/opt/pista-it/iceberg-runtime.jar"))

    val args = new scala.collection.mutable.ArrayBuffer[String]()
    args ++= Seq(
      sparkSubmit,
      "--class", "com.pista.spark.sql.batch.SparkSQLSubmitter",
      "--master", "local[*]",
      "--files", "/opt/pista-it/query.sql")
    icebergJar.foreach { _ =>
      args += "--jars"
      args += "/opt/pista-it/iceberg-runtime.jar"
    }
    args += "--conf"
    args += "spark.pista.sqlFile_=query.sql"
    params.asScala.toSeq.sortBy(_._1).foreach { case (key, value) =>
      args += "--conf"
      args += s"spark.pista.params.$key=$value"
    }
    submitterConf.asScala.toSeq.sortBy(_._1).foreach { case (key, value) =>
      args += "--conf"
      args += s"$key=$value"
    }
    args += "/opt/pista-it/pista-batch.jar"

    execWithTimeout(args.toSeq)
  }

  private def execWithTimeout(args: Seq[String]): Container.ExecResult = {
    val timeoutSeconds = sys.props.getOrElse("pista.it.submit.timeout.seconds", "600").toLong
    require(timeoutSeconds > 0, "pista.it.submit.timeout.seconds must be positive")
    val executor = Executors.newSingleThreadExecutor((runnable: Runnable) => {
      val thread = new Thread(runnable, s"$resourcePrefix-spark-submit")
      thread.setDaemon(true)
      thread
    })
    val future = executor.submit(new Callable[Container.ExecResult] {
      override def call(): Container.ExecResult = execInContainer(args: _*)
    })
    try future.get(timeoutSeconds, TimeUnit.SECONDS)
    catch {
      case _: TimeoutException =>
        future.cancel(true)
        throw new IllegalStateException(
          s"spark-submit did not finish within ${timeoutSeconds}s in $resourcePrefix")
    } finally executor.shutdownNow()
  }

  def captureLogs(): Unit =
    ContainerLogUtils.capture(getDockerClient, getContainerId, s"$resourcePrefix-spark")

  override def close(): Unit = synchronized {
    if (closed) return
    closed = true
    try stop()
    catch {
      case e: com.github.dockerjava.api.exception.DockerException
          if e.getHttpStatus == 304 || e.getHttpStatus == 404 =>
    }
  }
}
