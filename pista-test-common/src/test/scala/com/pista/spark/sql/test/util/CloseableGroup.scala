package com.pista.spark.sql.test.util

import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer

/**
 * Root-level resource group for one Maven/ScalaTest fork.
 *
 * Resources are closed in reverse registration order. A failure never prevents
 * later resources from being closed; all failures are reported together.
 */
final class CloseableGroup private () extends AutoCloseable {
  private val resources = ListBuffer.empty[AutoCloseable]
  private val closed = new AtomicBoolean(false)

  def register[T <: AutoCloseable](resource: T): T = synchronized {
    require(resource != null, "resource must not be null")
    if (closed.get()) {
      resource.close()
      throw new IllegalStateException("Cannot register a resource after CloseableGroup is closed")
    }
    resources += resource
    resource
  }

  /** Register only the exact path supplied by the current test run. */
  def registerPath(path: Path): Path = {
    require(path != null, "path must not be null")
    register(new AutoCloseable {
      override def close(): Unit = deleteExactPath(path)
    })
    path
  }

  override def close(): Unit = closeAll()

  def closeAll(): Unit = {
    if (!closed.compareAndSet(false, true)) return

    val failures = ListBuffer.empty[Throwable]
    resources.synchronized {
      resources.reverseIterator.foreach { resource =>
        try resource.close()
        catch { case t: Throwable => failures += t }
      }
      resources.clear()
    }

    if (failures.nonEmpty) {
      val error = new RuntimeException(
        s"Failed to close ${failures.size} resource(s) in CloseableGroup")
      failures.drop(1).foreach(error.addSuppressed)
      error.initCause(failures.head)
      throw error
    }
  }

  private def deleteExactPath(path: Path): Unit = {
    if (!Files.exists(path)) return
    val stream = Files.walk(path)
    try stream.sorted(Comparator.reverseOrder[Path]()).forEach(p => Files.deleteIfExists(p))
    finally stream.close()
  }
}

object CloseableGroup {
  def create(): CloseableGroup = new CloseableGroup()
}
