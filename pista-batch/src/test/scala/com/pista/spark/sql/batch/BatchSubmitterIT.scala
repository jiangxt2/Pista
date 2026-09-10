package com.pista.spark.sql.batch

import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.SubmitterIT
import org.scalactic.source.Position
import org.scalatest.Tag
import org.testcontainers.containers.Container

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util

/** Common host-side setup for Submitter ITs running inside Spark 3.5.8. */
abstract class BatchSubmitterIT extends BaseIT {
  protected lazy val sparkRuntime = ContainerSuite.spark

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
                  (implicit pos: Position): Unit =
    super.test(testName, (testTags :+ SubmitterIT): _*)(testFun)(pos)

  override def beforeAll(): Unit = {
    submitterJarPath
    super.beforeAll()
  }

  protected def submit(sql: String, conf: Map[String, String]): Unit = {
    val result = submitResult(sql, conf)
    if (result.getExitCode != 0) {
      throw new IllegalStateException(
        s"spark-submit failed with exit=${result.getExitCode}\nstdout=${result.getStdout}\nstderr=${result.getStderr}")
    }
  }

  protected def submitResult(sql: String, conf: Map[String, String]): Container.ExecResult = {
    submitterJarPath
    val file = Files.createTempFile("pista-it-submit-", ".sql")
    ContainerSuite.registerPath(file)
    Files.write(file, sql.getBytes(StandardCharsets.UTF_8))
    val params = new util.HashMap[String, String]()
    val submitterConf = new util.HashMap[String, String]()
    conf.foreach { case (key, value) => submitterConf.put(key, value) }
    sparkRuntime.submitResult(file.toString, params, submitterConf)
  }

  protected lazy val submitterJarPath = {
    val jarPath = sys.props.get("pista.it.submitter.jar")
      .map(_.trim)
      .filter(value => value.nonEmpty && !value.equalsIgnoreCase("null"))
      .getOrElse(
        throw new IllegalStateException(
          "Submitter IT requires -Dpista.it.submitter.jar pointing to the assembled pista-assembly JAR"))
    val path = Paths.get(jarPath)
    require(
      Files.isRegularFile(path),
      s"Submitter JAR does not exist or is not a file: $jarPath")
    path
  }
}
