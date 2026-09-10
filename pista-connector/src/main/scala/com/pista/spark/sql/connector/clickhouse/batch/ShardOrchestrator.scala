package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.connector.BatchWriteResult
import org.apache.spark.internal.Logging

import java.util.concurrent.{Callable, Executors, Future, TimeUnit}
import scala.collection.mutable

/**
 * Sharding writer coordinator
 *
 * Package retry and scheduling logic for concurrent shard execution for reuse by ClickHouseConcurrentWriter and ClickHouseBatchSubmitter.
 *
 * @param machineCount    shard count
 * @param maxThreads      Maximum concurrent threads
 * @param maxRetries      Maximum shard retry attempts for failures
 * @param logPrefix       logPrefix (such as "[ClickHouseConcurrentWriter]")
 * @param executeShard    Execute a single shard (parameters: shard index, verbose flag)
 * @param onShardStart Optional hook: Invoked before a shard starts execution.
 * @param onShardComplete Optional callback: called after a shard execution completes (whether successful or not)
 */
class ShardOrchestrator(
  machineCount: Int,
  maxThreads: Int,
  maxRetries: Int,
  logPrefix: String,
  executeShard: (Int, Boolean) => BatchWriteResult,
  onShardStart:    Int => Unit = _ => (),
  onShardComplete: (Int, BatchWriteResult) => Unit = (_, _) => ()
) extends Logging {

  def run(): Seq[BatchWriteResult] = {
    val pool    = Executors.newFixedThreadPool(Math.min(machineCount, maxThreads))
    val results = mutable.Map.empty[Int, BatchWriteResult]

    var attempt = 1
    var allDone  = false

    try {
      while (attempt <= maxRetries && !allDone) {
        val pending = (0 until machineCount).filterNot(i => results.get(i).exists(_.success))

        if (pending.isEmpty) {
          logInfo(s"$logPrefix All shards succeeded after $attempt attempt(s)")
          allDone = true
        } else {
          logInfo(s"$logPrefix Attempt $attempt/$maxRetries: ${pending.size} shards pending")

          val logShardIndex = pending.min

          val futures: Seq[(Int, Future[BatchWriteResult])] = pending.map { index =>
            val isLogShard = index == logShardIndex
            val task       = new Callable[BatchWriteResult] {
              def call(): BatchWriteResult = {
                onShardStart(index)
                val result = executeShard(index, isLogShard)
                onShardComplete(index, result)
                result
              }
            }
            index -> pool.submit(task)
          }

          futures.foreach { case (index, f) => results(index) = f.get() }

          val failed = results.values.count(!_.success)
          if (failed > 0)
            logWarning(s"$logPrefix $failed shard(s) failed in attempt $attempt")

          attempt += 1
        }
      }
    } finally {
      pool.shutdown()
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        logWarning(s"$logPrefix Thread pool did not terminate in 60s, waiting additional 5 minutes")
        pool.awaitTermination(300, TimeUnit.SECONDS)
      }
    }

    results.values.toSeq.sortBy(_.index)
  }
}
