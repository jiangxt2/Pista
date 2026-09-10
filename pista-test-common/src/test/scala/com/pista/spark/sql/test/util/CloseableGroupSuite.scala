package com.pista.spark.sql.test.util

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

class CloseableGroupSuite extends AnyFunSuite {
  test("resources close in reverse order and closeAll is idempotent") {
    val events = ArrayBuffer.empty[String]
    val group = CloseableGroup.create()
    group.register(new AutoCloseable { override def close(): Unit = events += "first" })
    group.register(new AutoCloseable { override def close(): Unit = events += "second" })

    group.closeAll()
    group.closeAll()

    assert(events == Seq("second", "first"))
  }

  test("a close failure does not prevent later resources from closing") {
    val closed = ArrayBuffer.empty[String]
    val group = CloseableGroup.create()
    group.register(new AutoCloseable {
      override def close(): Unit = {
        closed += "failing"
        throw new RuntimeException("expected")
      }
    })
    group.register(new AutoCloseable { override def close(): Unit = closed += "healthy" })

    intercept[RuntimeException](group.closeAll())
    assert(closed.toSet == Set("failing", "healthy"))
  }
}
