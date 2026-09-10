package com.pista.spark.sql.test.container

import com.pista.spark.sql.test.util.CloseableGroup
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.output.OutputFrame

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class ContainerLogUtilsSuite extends AnyFunSuite {
  test("Doris readiness accepts only positive reported storage capacity") {
    assert(PistaDorisContainer.hasPositiveCapacity("812.11 GB"))
    assert(PistaDorisContainer.hasPositiveCapacity("1 B"))
    assert(!PistaDorisContainer.hasPositiveCapacity("0.000 B"))
    assert(!PistaDorisContainer.hasPositiveCapacity("N/A"))
    assert(!PistaDorisContainer.hasPositiveCapacity(null))
  }

  test("streamToReport persists output frames before container startup completes") {
    val property = "pista.it.reports.dir"
    val previous = sys.props.get(property)
    val cleanup = CloseableGroup.create()
    val reports = cleanup.registerPath(Files.createTempDirectory("pista-log-consumer"))
    sys.props.put(property, reports.toString)

    try {
      val consumer = ContainerLogUtils.streamToReport("doris-fe")
      consumer.accept(new OutputFrame(OutputFrame.OutputType.STDOUT,
        "first\n".getBytes(StandardCharsets.UTF_8)))
      consumer.accept(new OutputFrame(OutputFrame.OutputType.STDERR,
        "second\n".getBytes(StandardCharsets.UTF_8)))
      consumer.accept(OutputFrame.END)

      val log = ContainerLogUtils.reportPath("doris-fe")
      assert(log == Paths.get(reports.toString, "pista-it", ContainerSuite.currentRunId,
        "doris-fe.log"))
      assert(Files.readString(log) == "first\nsecond\n")
    } finally {
      previous match {
        case Some(value) => sys.props.put(property, value)
        case None        => sys.props.remove(property)
      }
      cleanup.closeAll()
    }
  }
}
