package com.pista.spark.sql.metrics.writer

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
 * discard strategy
 *
 */
sealed trait DropPolicy

object DropPolicy {
  /** Drop the oldest elements when the queue is full */
  case object DropOldest extends DropPolicy

  /** Drop the latest element when the queue is full (the current element being inserted) */
  case object DropNewest extends DropPolicy

  def fromString(s: String): DropPolicy = s.toLowerCase match {
    case "newest" => DropNewest
    case _ => DropOldest
  }
}

/**
 * bounded metrics queue
 *
 * thread-safe bounded queue supporting non-blocking enqueue and batch dequeue.
 * When the queue is full, decide which element to discard based on the discard policy.
 *
 * @param capacity   Queue capacity
 * @param dropPolicy dropPolicy
 * @tparam T Element type
 */
class MetricsQueue[T](val capacity: Int, dropPolicy: DropPolicy) {

  private val queue = new LinkedBlockingQueue[T](capacity)
  private val droppedCounter = new AtomicLong(0)

  /**
   * Non-blocking enqueue
   *
   * @param item The element to enqueue.
   * @return true If the element is successfully enqueued, false if it is discarded.
   */
  def offer(item: T): Boolean = {
    if (queue.offer(item)) {
      true
    } else {
      droppedCounter.incrementAndGet()
      dropPolicy match {
        case DropPolicy.DropOldest =>
          queue.poll() // Discard the oldest
          queue.offer(item)
        case DropPolicy.DropNewest =>
          false // discard the current one
      }
    }
  }

  /**
   * batch dequeue
   *
   * @param maxElements Maximum dequeued elements
   * @return The list of elements dequeued.
   */
  def drain(maxElements: Int): Seq[T] = {
    val buffer = new java.util.ArrayList[T](maxElements)
    queue.drainTo(buffer, maxElements)
    buffer.asScala.toSeq
  }

  /**
   * Get current queue size
   */
  def size: Int = queue.size()

  /**
   * Return the total number of discarded elements.
   */
  def droppedCount: Long = droppedCounter.get()

  /**
   * Is the queue empty?
   */
  def isEmpty: Boolean = queue.isEmpty

  /**
   * clear queue
   */
  def clear(): Unit = queue.clear()
}
