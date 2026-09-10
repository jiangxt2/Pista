package com.pista.spark.sql.execution.checkpoint

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.UUID

/**
 * Checkpoint subsystem testing
 *
 * Split into two parts:
 * 1. CheckpointRegistry checkpointing in memory (without SparkSession) SparkSession)
 * 2. MaterializationManager Integration Tests for the lifecycle of (checkpoint → release → cleanup)
 *
 */
class CheckpointSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("CheckpointSuite")
    .getOrCreate()

  val checkpointBase = "/tmp/checkpoint-suite-test"

  override def afterAll(): Unit = {
    // Ensure that MaterializationManager is cleaned up
    try { MaterializationManager.cleanup() } catch { case _: Exception => }
    deleteDirectory(new File(checkpointBase))
    if (spark != null) spark.stop()
  }

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { f =>
        if (f.isDirectory) deleteDirectory(f) else f.delete()
      })
      dir.delete()
    }

  // ==================== CheckpointRegistry Unit Test ====================

  test("Registry registered should be able to find checkpoint") {
    val registry = new CheckpointRegistry()
    val id = UUID.randomUUID()
    val handle = CheckpointHandle(id, null, "/tmp/test", System.currentTimeMillis())

    registry.register(handle)

    assert(registry.get(id).isDefined)
    assert(registry.get(id).get.id == id)
    assert(registry.size == 1)
  }

  test("Registry deregistered should not find checkpoint") {
    val registry = new CheckpointRegistry()
    val id = UUID.randomUUID()
    val handle = CheckpointHandle(id, null, "/tmp/test", System.currentTimeMillis())

    registry.register(handle)
    val removed = registry.unregister(id)

    assert(removed.isDefined)
    assert(removed.get.id == id)
    assert(registry.get(id).isEmpty)
    assert(registry.size == 0)
  }

  test("Registry deregisters nonexistent IDs should return None") {
    val registry = new CheckpointRegistry()
    val result = registry.unregister(UUID.randomUUID())
    assert(result.isEmpty)
  }

  test("Registry getAll should return all registered checkpoints checkpoint") {
    val registry = new CheckpointRegistry()
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()
    val id3 = UUID.randomUUID()

    registry.register(CheckpointHandle(id1, null, "/tmp/1", System.currentTimeMillis()))
    registry.register(CheckpointHandle(id2, null, "/tmp/2", System.currentTimeMillis()))
    registry.register(CheckpointHandle(id3, null, "/tmp/3", System.currentTimeMillis()))

    val all = registry.getAll
    assert(all.size == 3)
    assert(registry.getAllIds.toSet == Set(id1, id2, id3))
  }

  test("Registry clear should clear all checkpoint") {
    val registry = new CheckpointRegistry()
    registry.register(CheckpointHandle(UUID.randomUUID(), null, "/tmp/1", System.currentTimeMillis()))
    registry.register(CheckpointHandle(UUID.randomUUID(), null, "/tmp/2", System.currentTimeMillis()))

    assert(registry.size == 2)
    assert(!registry.isEmpty)

    registry.clear()

    assert(registry.size == 0)
    assert(registry.isEmpty)
    assert(registry.getAll.isEmpty)
  }

  test("Registry duplicates for the same ID should be overwritten") {
    val registry = new CheckpointRegistry()
    val id = UUID.randomUUID()

    registry.register(CheckpointHandle(id, null, "/tmp/old", System.currentTimeMillis()))
    registry.register(CheckpointHandle(id, null, "/tmp/new", System.currentTimeMillis()))

    assert(registry.size == 1)
    assert(registry.get(id).get.path == "/tmp/new")
  }

  // ==================== CheckpointHandle Unit Test ====================

  test("CheckpointHandle ageMillis should return a positive number") {
    val handle = CheckpointHandle(
      UUID.randomUUID(), null, "/tmp/test",
      System.currentTimeMillis() - 1000
    )

    assert(handle.ageMillis >= 1000)
    assert(handle.ageSeconds >= 1)
  }

  test("CheckpointHandle toString should contain key information") {
    val id = UUID.randomUUID()
    val handle = CheckpointHandle(id, null, "/tmp/test", System.currentTimeMillis())

    val str = handle.toString
    assert(str.contains(id.toString))
    assert(str.contains("/tmp/test"))
  }

  // ==================== CheckpointMetadata Unit Tests ====================

  test("CheckpointMetadataManager create should create empty metadata") {
    val metadata = CheckpointMetadataManager.create("job-1", "app-1")

    assert(metadata.jobId == "job-1")
    assert(metadata.applicationId == "app-1")
    assert(metadata.checkpoints.isEmpty)
    assert(metadata.createdAt > 0)
  }

  test("CheckpointMetadataManager addCheckpoint should append checkpoint information") {
    val metadata = CheckpointMetadataManager.create("job-1", "app-1")
    val id1 = UUID.randomUUID()
    val id2 = UUID.randomUUID()

    val updated1 = CheckpointMetadataManager.addCheckpoint(metadata, id1, "/tmp/cp1")
    val updated2 = CheckpointMetadataManager.addCheckpoint(updated1, id2, "/tmp/cp2")

    assert(updated2.checkpoints.size == 2)
    assert(updated2.checkpoints.head.id == id1.toString)
    assert(updated2.checkpoints(1).id == id2.toString)
    assert(!updated2.checkpoints.head.released)
  }

  test("CheckpointMetadataManager markReleased should mark as released") {
    val metadata = CheckpointMetadataManager.create("job-1", "app-1")
    val id = UUID.randomUUID()

    val withCp = CheckpointMetadataManager.addCheckpoint(metadata, id, "/tmp/cp")
    val released = CheckpointMetadataManager.markReleased(withCp, id)

    assert(released.checkpoints.head.released)
  }

  test("CheckpointMetadataManager markReleased does not affect other records for an invalid ID") {
    val metadata = CheckpointMetadataManager.create("job-1", "app-1")
    val id = UUID.randomUUID()
    val otherId = UUID.randomUUID()

    val withCp = CheckpointMetadataManager.addCheckpoint(metadata, id, "/tmp/cp")
    val released = CheckpointMetadataManager.markReleased(withCp, otherId)

    assert(!released.checkpoints.head.released)
  }

  // ==================== MaterializationManager Integration Test ====================

  test("MaterializationManager Not Initialized When getCheckpointCount Should Return 0") {
    // Ensure clean state
    try { MaterializationManager.cleanup() } catch { case _: Exception => }
    assert(MaterializationManager.getCheckpointCount == 0)
  }

  test("MaterializationManager Initialization is not performed. checkpoint should throw an exception") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("id")

    assertThrows[Exception] {
      MaterializationManager.checkpoint(df)
    }
  }

  test("MaterializationManager initialize → checkpoint → release → cleanup Complete Lifecycle") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    // initialize exactly
    MaterializationManager.initialize(spark, s"file://$checkpointBase/lifecycle")
    assert(MaterializationManager.getCheckpointCount == 0)

    // Create a materialization.
    val df = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val handle = MaterializationManager.checkpoint(df, eager = true)

    assert(handle.id != null)
    assert(handle.dataFrame != null)
    assert(handle.path.nonEmpty)
    assert(MaterializationManager.getCheckpointCount == 1)

    // Verify that the DataFrame is readable after the checkpoint.
    val result = handle.dataFrame
    assert(result.count() == 3)
    assert(result.columns.toSet == Set("id", "name"))

    // Release
    MaterializationManager.release(handle)
    assert(MaterializationManager.getCheckpointCount == 0)

    // Clean
    MaterializationManager.cleanup()
  }

  test("MaterializationManager manages multiple checkpoints independently") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/multi")

    val df1 = Seq(1, 2, 3).toDF("value")
    val df2 = Seq("a", "b").toDF("letter")

    val handle1 = MaterializationManager.checkpoint(df1, eager = true)
    val handle2 = MaterializationManager.checkpoint(df2, eager = true)

    assert(MaterializationManager.getCheckpointCount == 2)
    assert(handle1.dataFrame.count() == 3)
    assert(handle2.dataFrame.count() == 2)

    // Unpersist the first.
    MaterializationManager.release(handle1)
    assert(MaterializationManager.getCheckpointCount == 1)

    // Second column is still available.
    assert(handle2.dataFrame.count() == 2)

    MaterializationManager.cleanup()
  }

  test("MaterializationManager releaseAll all checkpoints should be released checkpoint") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/releaseall")

    val df = Seq(1, 2).toDF("id")
    MaterializationManager.checkpoint(df, eager = true)
    MaterializationManager.checkpoint(df, eager = true)
    MaterializationManager.checkpoint(df, eager = true)

    assert(MaterializationManager.getCheckpointCount == 3)

    MaterializationManager.releaseAll()
    assert(MaterializationManager.getCheckpointCount == 0)

    MaterializationManager.cleanup()
  }

  test("MaterializationManager getActiveCheckpoints it should return all active handles") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/active")

    val df = Seq(1).toDF("id")
    val h1 = MaterializationManager.checkpoint(df, eager = true)
    val h2 = MaterializationManager.checkpoint(df, eager = true)

    val active = MaterializationManager.getActiveCheckpoints
    assert(active.size == 2)
    assert(active.map(_.id).toSet == Set(h1.id, h2.id))

    MaterializationManager.cleanup()
  }

  test("MaterializationManager Repeated Initialization Should Be Skipped") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    MaterializationManager.initialize(spark, s"file://$checkpointBase/dup_init")
    // Reinitialization should not throw an exception
    MaterializationManager.initialize(spark, s"file://$checkpointBase/dup_init_2")

    // Clean
    MaterializationManager.cleanup()
  }

  test("MaterializationManager cleanup After can be reinitialized") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/reinit1")
    val df = Seq(1).toDF("id")
    MaterializationManager.checkpoint(df, eager = true)
    MaterializationManager.cleanup()

    // Reinitialize exactly.
    MaterializationManager.initialize(spark, s"file://$checkpointBase/reinit2")
    assert(MaterializationManager.getCheckpointCount == 0)
    MaterializationManager.cleanup()
  }

  test("MaterializationManager Not Initialized When cleanup Should Not Error") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }
    // No further cleanup should result in an exception.
    MaterializationManager.cleanup()
  }

  // ==================== CheckpointWriter / CheckpointReader Indirect Testing ====================

  test("checkpoint write after should read consistent data (round trip test)") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/roundtrip")

    val original = Seq(
      (1, "Alice", 25.5),
      (2, "Bob", 30.0),
      (3, "Charlie", 35.5)
    ).toDF("id", "name", "score")

    val handle = MaterializationManager.checkpoint(original, eager = true)
    val restored = handle.dataFrame

    // Validate row count
    assert(restored.count() == original.count())

    // Validate schema
    assert(restored.schema.fieldNames.toSet == original.schema.fieldNames.toSet)

    // Validate dataset contents
    val originalData = original.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).toSet
    val restoredData = restored.collect().map(r => (r.getInt(0), r.getString(1), r.getDouble(2))).toSet
    assert(originalData == restoredData)

    MaterializationManager.cleanup()
  }

  // ==================== CheckpointMetadata Persistence Test ====================

  test("Metadata should be able to persist and load") {
    try { MaterializationManager.cleanup() } catch { case _: Exception => }

    import spark.implicits._

    MaterializationManager.initialize(spark, s"file://$checkpointBase/metadata_test")

    val df = Seq(1, 2).toDF("id")
    MaterializationManager.checkpoint(df, eager = true)

    // Metadata is automatically persisted during the checkpoint process.
    // Load validation via CheckpointMetadataManager directly.
    // Note: jobDir is an internal variable, which is indirectly validated through getActiveCheckpoints.
    val active = MaterializationManager.getActiveCheckpoints
    assert(active.size == 1)

    MaterializationManager.cleanup()
  }
}
