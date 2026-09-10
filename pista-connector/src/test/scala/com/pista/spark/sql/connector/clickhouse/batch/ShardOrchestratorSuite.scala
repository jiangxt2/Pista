package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.connector.BatchWriteResult
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class ShardOrchestratorSuite extends AnyFunSuite {

  private def alwaysSucceed(machineCount: Int): Seq[BatchWriteResult] =
    new ShardOrchestrator(
      machineCount = machineCount,
      maxThreads   = 2,
      maxRetries   = 3,
      logPrefix    = "[Test]",
      executeShard = (i, _) => BatchWriteResult(i, 100L, success = true)
    ).run()

  test("All shards succeed for the first time, return all success") {
    val results = alwaysSucceed(3)
    assert(results.size == 3)
    assert(results.forall(_.success))
    assert(results.map(_.index).sorted == Seq(0, 1, 2))
  }

  test("Shard Retry Failed Initially, Success Subsequently") {
    val attemptCount = mutable.Map(0 -> 0, 1 -> 0)
    val results = new ShardOrchestrator(
      machineCount = 2,
      maxThreads   = 2,
      maxRetries   = 3,
      logPrefix    = "[Test]",
      executeShard = { (i, _) =>
        attemptCount(i) += 1
        // Shard 1 First failure, second success.
        val success = i == 0 || attemptCount(i) >= 2
        BatchWriteResult(i, if (success) 50L else 0L, success)
      }
    ).run()

    assert(results.forall(_.success))
    assert(attemptCount(0) == 1, "Shard 0 should execute only once")
    assert(attemptCount(1) == 2, "Shard 1 should execute twice (failure in round one + retry)")
  }

  test("exceeding maxRetries and still fails, includes success=false") {
    val results = new ShardOrchestrator(
      machineCount = 2,
      maxThreads   = 2,
      maxRetries   = 2,
      logPrefix    = "[Test]",
      executeShard = { (i, _) =>
        // Shard 1 always fails.
        val success = i == 0
        BatchWriteResult(i, if (success) 100L else 0L, success, errorMessage = if (!success) "write error" else "")
      }
    ).run()

    assert(results.find(_.index == 0).exists(_.success))
    assert(results.find(_.index == 1).exists(!_.success))
    assert(results.find(_.index == 1).exists(_.errorMessage.nonEmpty))
  }

  test("onShardStart and onShardComplete are invoked before and after each shard execution") {
    val startLog    = mutable.ListBuffer.empty[Int]
    val completeLog = mutable.ListBuffer.empty[Int]

    new ShardOrchestrator(
      machineCount    = 3,
      maxThreads      = 1,
      maxRetries      = 1,
      logPrefix       = "[Test]",
      executeShard    = (i, _) => BatchWriteResult(i, 1L, success = true),
      onShardStart    = startLog += _,
      onShardComplete = (i, _) => completeLog += i
    ).run()

    assert(startLog.sorted == Seq(0, 1, 2))
    assert(completeLog.sorted == Seq(0, 1, 2))
  }

  test("Results sorted by shard index in ascending order") {
    val results = alwaysSucceed(5)
    assert(results.map(_.index) == results.map(_.index).sorted)
  }

  test("machineCount=1 boundary conditions") {
    val results = alwaysSucceed(1)
    assert(results.size == 1)
    assert(results.head.success)
    assert(results.head.index == 0)
  }
}
