package com.pista.spark.sql.batch

import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite

/** Verifies the Spark Submitter runtime image used by container-backed ITs. */
class SparkContainerIT extends BaseIT {

  test("Spark container uses version 3.5.8 and is executable with spark-submit") {
    val result = ContainerSuite.spark.execInContainer("/opt/spark/bin/spark-submit", "--version")
    val output = result.getStdout + result.getStderr
    assert(result.getExitCode == 0, output)
    assert(output.contains("3.5.8"), s"Spark runtime version mismatch: $output")
  }
}
