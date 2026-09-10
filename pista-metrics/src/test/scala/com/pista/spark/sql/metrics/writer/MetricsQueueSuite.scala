package com.pista.spark.sql.metrics.writer

import com.pista.spark.sql.metrics.model._
import org.scalatest.funsuite.AnyFunSuite

/**
 * MetricsQueue Queue Behavior Test
 *
 **Coverage: FIFO/FIFO strategy, DropOldest/DropNewest policies, and boundary conditions.**
 * Attention: The MetricsSuite already has 3 base tests, this Suite supplements boundary and concurrency scenarios.
 *
 */
class MetricsQueueSuite extends AnyFunSuite {

  test("Empty queue drain should return an empty list") {
    val queue = new MetricsQueue[String](10, DropPolicy.DropOldest)
    assert(queue.drain(5).isEmpty)
    assert(queue.isEmpty)
    assert(queue.size == 0)
  }

  test("drain maxElements return all elements when drain exceeds queue size") {
    val queue = new MetricsQueue[Int](10, DropPolicy.DropOldest)
    queue.offer(1)
    queue.offer(2)
    queue.offer(3)

    val drained = queue.drain(100)
    assert(drained == Seq(1, 2, 3))
    assert(queue.isEmpty)
  }

  test("drain maxElements set to 0 should return an empty list") {
    val queue = new MetricsQueue[Int](10, DropPolicy.DropOldest)
    queue.offer(1)

    val drained = queue.drain(0)
    assert(drained.isEmpty)
    assert(queue.size == 1)
  }

  test("Queue with capacity of 1 and DropOldest policy always retains the latest element") {
    val queue = new MetricsQueue[Int](1, DropPolicy.DropOldest)

    queue.offer(1)
    queue.offer(2)
    queue.offer(3)

    assert(queue.size == 1)
    assert(queue.droppedCount == 2)
    assert(queue.drain(1) == Seq(3))
  }

  test("Queue with capacity of 1 using DropNewest should always retain the oldest element") {
    val queue = new MetricsQueue[Int](1, DropPolicy.DropNewest)

    queue.offer(1)
    queue.offer(2)
    queue.offer(3)

    assert(queue.size == 1)
    assert(queue.droppedCount == 2)
    assert(queue.drain(1) == Seq(1))
  }

  test("clear should empty the queue but not reset droppedCount") {
    val queue = new MetricsQueue[Int](2, DropPolicy.DropOldest)
    queue.offer(1)
    queue.offer(2)
    queue.offer(3) // Trigger drop

    assert(queue.droppedCount == 1)
    queue.clear()
    assert(queue.isEmpty)
    assert(queue.droppedCount == 1) // droppedCount does not reset
  }

  test("DropPolicy.fromString should correctly parse") {
    assert(DropPolicy.fromString("newest") == DropPolicy.DropNewest)
    assert(DropPolicy.fromString("oldest") == DropPolicy.DropOldest)
    assert(DropPolicy.fromString("NEWEST") == DropPolicy.DropNewest)
    assert(DropPolicy.fromString("unknown") == DropPolicy.DropOldest) // default
  }

  test("drain multiple should return in FIFO order") {
    val queue = new MetricsQueue[Int](10, DropPolicy.DropOldest)
    (1 to 6).foreach(queue.offer)

    val batch1 = queue.drain(3)
    val batch2 = queue.drain(3)

    assert(batch1 == Seq(1, 2, 3))
    assert(batch2 == Seq(4, 5, 6))
    assert(queue.isEmpty)
  }
}
