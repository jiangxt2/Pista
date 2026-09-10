package com.pista.spark.sql.execution.checkpoint

import org.apache.spark.sql.DataFrame
import java.util.UUID

/**
 * Checkpoint handle
 *
 * Users access the DataFrame checkpointed by this handle and can manually release resources.
 *
 * @param id checkpoint Unique identifier
 * @param dataFrame checkpointed DataFrame
 * @param path checkpoint storage path
 * @param createdAt creation_timestamp
 *
 */
case class CheckpointHandle(
  id: UUID,
  dataFrame: DataFrame,
  path: String,
  createdAt: Long
) {

  /**
   * checkpoint overwrite exactly
   *
   * Delete the corresponding storage directory and remove it from the registry.
   * This handle will be invalid after this statement, it should not be used anymore.
   */
  def release(): Unit = {
    MaterializationManager.release(this)
  }

  /**
   * Get the age of the checkpoint (milliseconds)
   */
  def ageMillis: Long = System.currentTimeMillis() - createdAt

  /**
   * Get the age of the checkpoint in seconds
   */
  def ageSeconds: Long = ageMillis / 1000

  override def toString: String =
    s"CheckpointHandle(id=$id, path=$path, age=${ageSeconds}s)"
}
