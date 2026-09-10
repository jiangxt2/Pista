package com.pista.spark.sql.execution.checkpoint

import org.apache.spark.internal.Logging

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Checkpoint registry
 *
 * Thread-safe registry tracking all active checkpoints.
 *
 */
class CheckpointRegistry extends Logging {

  private val registry = new ConcurrentHashMap[UUID, CheckpointHandle]()

  /**
   * Register a checkpoint
   */
  def register(handle: CheckpointHandle): Unit = {
    registry.put(handle.id, handle)
    logInfo(s"Registered checkpoint: ${handle.id}")
  }

  /**
   * checkpoint overwrite
   */
  def unregister(id: UUID): Option[CheckpointHandle] = {
    Option(registry.remove(id)).map { handle =>
      logInfo(s"Unregistered checkpoint: $id")
      handle
    }
  }

  /**
   * Obtain specified checkpoint
   */
  def get(id: UUID): Option[CheckpointHandle] = {
    Option(registry.get(id))
  }

  /**
   * Get all checkpoints
   */
  def getAll: Seq[CheckpointHandle] = {
    registry.values().asScala.toSeq
  }

  /**
   * Get checkpoint count
   */
  def size: Int = registry.size()

  /**
   * Clear registry registration
   */
  def clear(): Unit = {
    val count = registry.size()
    registry.clear()
    logInfo(s"Cleared registry, removed $count checkpoints")
  }

  /**
   * Check for null
   */
  def isEmpty: Boolean = registry.isEmpty

  /**
   * Get all checkpoint IDs
   */
  def getAllIds: Seq[UUID] = {
    registry.keySet().asScala.toSeq
  }
}
