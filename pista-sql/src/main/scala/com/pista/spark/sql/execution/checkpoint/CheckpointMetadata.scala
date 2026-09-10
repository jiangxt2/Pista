package com.pista.spark.sql.execution.checkpoint

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.util.UUID

/**
 * Checkpoint metadata
 *
 * Persist and restore metadata for checkpoint records.
 *
 * @param jobId Job unique identifier
 * @param applicationId Spark Application ID
 * @param checkpoints checkpoint list
 * @param createdAt creation_timestamp
 *
 */
case class CheckpointMetadata(
  jobId: String,
  applicationId: String,
  checkpoints: Seq[CheckpointInfo],
  createdAt: Long
)

/**
 * Information for a single checkpoint*
 *
 * @param id checkpoint UUID
 * @param path checkpoint \ path
 * @param createdAt creation_timestamp
 * @param released Whether released
 */
case class CheckpointInfo(
  id: String,
  path: String,
  createdAt: Long,
  released: Boolean = false
)

/**
 * metadata manager
 *
 * Serialize, deserialize, and persist metadata.
 *
 */
object CheckpointMetadataManager {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val METADATA_FILE_NAME = ".metadata"

  /**
   * Persist metadata to file
   */
  def save(metadata: CheckpointMetadata, jobDir: String, spark: SparkSession): Unit = {
    val metadataPath = new Path(jobDir, METADATA_FILE_NAME)
    val fs = FileSystem.get(metadataPath.toUri, spark.sparkContext.hadoopConfiguration)

    val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metadata)
    val outputStream = fs.create(metadataPath, true)
    try {
      outputStream.write(json.getBytes("UTF-8"))
    } finally {
      outputStream.close()
    }
  }

  /**
   * Load metadata from file
   */
  def load(jobDir: String, spark: SparkSession): Option[CheckpointMetadata] = {
    val metadataPath = new Path(jobDir, METADATA_FILE_NAME)
    val fs = FileSystem.get(metadataPath.toUri, spark.sparkContext.hadoopConfiguration)

    if (fs.exists(metadataPath)) {
      val inputStream = fs.open(metadataPath)
      try {
        val json = scala.io.Source.fromInputStream(inputStream).mkString
        Some(mapper.readValue(json, classOf[CheckpointMetadata]))
      } finally {
        inputStream.close()
      }
    } else {
      None
    }
  }

  /**
   * Create new metadata
   */
  def create(jobId: String, applicationId: String): CheckpointMetadata = {
    CheckpointMetadata(
      jobId = jobId,
      applicationId = applicationId,
      checkpoints = Seq.empty,
      createdAt = System.currentTimeMillis()
    )
  }

  /**
   * add checkpoint information
   */
  def addCheckpoint(
    metadata: CheckpointMetadata,
    id: UUID,
    path: String
  ): CheckpointMetadata = {
    val info = CheckpointInfo(
      id = id.toString,
      path = path,
      createdAt = System.currentTimeMillis(),
      released = false
    )
    metadata.copy(checkpoints = metadata.checkpoints :+ info)
  }

  /**
   * checkpoint mark as released
   */
  def markReleased(metadata: CheckpointMetadata, id: UUID): CheckpointMetadata = {
    val updatedCheckpoints = metadata.checkpoints.map { info =>
      if (info.id == id.toString) info.copy(released = true) else info
    }
    metadata.copy(checkpoints = updatedCheckpoints)
  }
}
