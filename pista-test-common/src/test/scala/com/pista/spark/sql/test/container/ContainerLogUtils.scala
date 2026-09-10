package com.pista.spark.sql.test.container

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.model.Frame
import org.testcontainers.containers.output.OutputFrame

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/** Writes Docker logs to a run-scoped directory below the current module's test reports directory. */
object ContainerLogUtils {
  private val captureTimeoutSeconds = 10L

  /** Persist Docker output while a container is starting so failed-start logs survive cleanup. */
  def streamToReport(name: String): Consumer[OutputFrame] = {
    val output = reportPath(name)
    Files.createDirectories(output.getParent)
    Files.write(output, Array.empty[Byte],
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    new Consumer[OutputFrame] {
      override def accept(frame: OutputFrame): Unit = synchronized {
        val bytes = frame.getBytes
        if (bytes != null && bytes.nonEmpty)
          Files.write(output, bytes, StandardOpenOption.APPEND)
      }
    }
  }

  def capture(client: DockerClient, containerId: String, name: String): Unit = {
    if (client == null || containerId == null || containerId.isEmpty) return
    val output = reportPath(name)
    Files.createDirectories(output.getParent)
    val stream = Files.newOutputStream(output,
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    val callback = new ResultCallback.Adapter[Frame] {
      override def onNext(frame: Frame): Unit = stream.write(frame.getPayload)
    }
    try {
      client.logContainerCmd(containerId)
        .withStdOut(true)
        .withStdErr(true)
        .withTimestamps(true)
        .withFollowStream(false)
        .withTailAll()
        .exec(callback)
      callback.awaitCompletion(captureTimeoutSeconds, TimeUnit.SECONDS)
    } catch {
      case error: Throwable =>
        val message = s"\n[PistaIT] Docker log capture failed: ${error.getMessage}\n"
        stream.write(message.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    } finally {
      try callback.close()
      finally stream.close()
    }
  }

  private[container] def reportPath(name: String): Path = {
    val reports = Paths.get(sys.props.getOrElse("pista.it.reports.dir",
      Paths.get("target", "surefire-reports").toString))
    reports.resolve("pista-it").resolve(ContainerSuite.currentRunId).resolve(s"$name.log")
  }
}
