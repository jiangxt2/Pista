package com.pista.spark.sql.test.container

import com.pista.spark.sql.test.util.CloseableGroup
import org.testcontainers.containers.Network

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

/** One lazily-created resource graph per forked test JVM. */
object ContainerSuite {
  private val runId = UUID.randomUUID().toString.replace("-", "").take(12)
  private val resourcePrefix = s"pista-it-$runId"
  private val closer = CloseableGroup.create()
  private val logCaptures = ListBuffer.empty[() => Unit]
  private val logsCaptured = new AtomicBoolean(false)

  def currentRunId: String = runId
  def registerPath(path: java.nio.file.Path): Unit = closer.registerPath(path)
  def closeAll(): Unit = {
    val failures = ListBuffer.empty[Throwable]
    if (logsCaptured.compareAndSet(false, true)) {
      logCaptures.synchronized {
        logCaptures.reverseIterator.foreach { capture =>
          try capture()
          catch { case error: Throwable => failures += error }
        }
        logCaptures.clear()
      }
    }
    try closer.closeAll()
    catch { case error: Throwable => failures += error }
    if (failures.nonEmpty) {
      val error = new RuntimeException(
        s"Failed to close/capture ${failures.size} Pista IT resource(s)")
      failures.drop(1).foreach(error.addSuppressed)
      error.initCause(failures.head)
      throw error
    }
  }

  private def registerLogCapture(capture: => Unit): Unit = logCaptures.synchronized {
    require(!logsCaptured.get(), "Cannot register log capture after cleanup has started")
    logCaptures += (() => capture)
  }

  sys.addShutdownHook(closeAll())

  lazy val network: Network = {
    val value = IsolatedNetworkFactory.create(resourcePrefix, runId)
    closer.register(value)
    value
  }

  lazy val clickhouse: PistaClickHouseContainer = {
    val value = new PistaClickHouseContainer(network, resourcePrefix)
    closer.register(value)
    registerLogCapture(value.captureLogs())
    value.start()
    value
  }

  lazy val postgres: PistaPostgresContainer = {
    val value = new PistaPostgresContainer(network, resourcePrefix)
    closer.register(value)
    registerLogCapture(value.captureLogs())
    value.start()
    value
  }

  lazy val minio: PistaMinIOContainer = {
    val value = new PistaMinIOContainer(network, resourcePrefix)
    closer.register(value)
    registerLogCapture(value.captureLogs())
    value.start()
    value
  }

  lazy val dorisNetwork: Network = {
    val value = IsolatedNetworkFactory.create(s"$resourcePrefix-doris", runId)
    closer.register(value)
    value
  }

  lazy val doris: PistaDorisContainer = {
    val value = new PistaDorisContainer(dorisNetwork, resourcePrefix)
    closer.register(value)
    value.start()
    value
  }

  /** Shared Spark runtime for Submitter IT; it joins only this run's networks. */
  lazy val spark: PistaSparkContainer = {
    val primaryNetwork = network
    val dataNetwork = dorisNetwork
    val value = new PistaSparkContainer(primaryNetwork, resourcePrefix)
    closer.register(value)
    registerLogCapture(value.captureLogs())
    value.start()
    value.connectToNetwork(dataNetwork)
    value
  }
}
