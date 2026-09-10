package com.pista.spark.sql.test.base

import com.pista.spark.sql.test.DockerIT
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, Tag}
import org.scalatest.funsuite.AnyFunSuite

/** Common lifecycle and Docker gate for all container-backed suites. */
trait BaseIT extends AnyFunSuite with BeforeAndAfterAll with Logging {
  override def test(testName: String, testTags: Tag*)(testFun: => Any)
                  (implicit pos: org.scalactic.source.Position): Unit =
    super.test(testName, (testTags :+ DockerIT): _*)(testFun)(pos)

  override def beforeAll(): Unit = {
    logInfo(s"[PistaIT] runId=${com.pista.spark.sql.test.container.ContainerSuite.currentRunId}")
    if (!sys.props.getOrElse("skipDockerCheck", "false").toBoolean) checkDockerAvailable()
    super.beforeAll()
  }

  private def checkDockerAvailable(): Unit = {
    try {
      val factory = org.testcontainers.DockerClientFactory.instance()
      val info = factory.client().infoCmd().exec()
      logInfo(s"[PistaIT] Docker server=${info.getServerVersion}")
    } catch {
      case e: Throwable =>
        throw new IllegalStateException(
          "Docker daemon is not available; run IT with Docker or set -DskipDockerCheck=true locally",
          e)
    }
  }
}
