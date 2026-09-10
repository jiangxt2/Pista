package com.pista.spark.sql.metrics

import com.pista.spark.sql.metrics.writer.DropPolicy
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
 * MetricsManager Control Flow Test
 *
 * Initialization/registration/shutdown lifecycle, configure driver,stats metrics retrieval
 *
 */
class MetricsManagerSuite extends AnyFunSuite with BeforeAndAfterEach {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("MetricsManagerSuite")
    .getOrCreate()

  val outputDir = "/tmp/metrics-manager-suite-test"

  override def afterEach(): Unit = {
    MetricsManager.shutdown()
    deleteDirectory(new File(outputDir))
  }

  private def deleteDirectory(dir: File): Unit =
    if (dir.exists()) {
      Option(dir.listFiles()).foreach(_.foreach { f =>
        if (f.isDirectory) deleteDirectory(f) else f.delete()
      })
      dir.delete()
    }

  private def defaultConfig = MetricsConfig(
    enabled = true,
    outputDir = outputDir,
    queueCapacity = 100,
    dropPolicy = DropPolicy.DropOldest,
    batchSize = 10,
    flushIntervalMs = 100,
    executorEnabled = false,
    skewEnabled = false,
    skewThreshold = 3.0,
    skewTopN = 10,
    shuffleEnabled = false,
    errorEnabled = false,
    planEnabled = false,
    planTruncateLength = 10000,
    dqMaxColumns = 50
  )

  test("register should register a listener and start a writer") {
    val manager = new MetricsManager(spark, defaultConfig)
    assert(!manager.isRegistered)

    manager.register()
    assert(manager.isRegistered)

    manager.close()
    assert(!manager.isRegistered)
  }

  test("register disabled should not register") {
    val config = defaultConfig.copy(enabled = false)
    val manager = new MetricsManager(spark, config)

    manager.register()
    assert(!manager.isRegistered)
  }

  test("repeated registrations are not detected register should not throw an error") {
    val manager = new MetricsManager(spark, defaultConfig)
    manager.register()
    manager.register() // Duplicate Registration
    assert(manager.isRegistered)

    manager.close()
  }

  test("close Closing without registration should not produce an error.") {
    val manager = new MetricsManager(spark, defaultConfig)
    manager.close() // Unregistered Shutdown
  }

  test("stats Registered should return Some") {
    val manager = new MetricsManager(spark, defaultConfig)
    manager.register()

    val stats = manager.stats
    assert(stats.isDefined)
    assert(stats.get.writeCount == 0)
    assert(stats.get.errorCount == 0)

    manager.close()
  }

  test("stats not registered should return None") {
    val manager = new MetricsManager(spark, defaultConfig)
    assert(manager.stats.isEmpty)
  }

  // ==================== MetricsManager Singleton Test ====================

  test("MetricsManager.initialize should create an instance") {
    // Through Spark configuration driver
    spark.conf.set("spark.pista.metrics.enabled", "true")
    spark.conf.set("spark.pista.metrics.output.path", outputDir)

    MetricsManager.initialize(spark)

    val instance = MetricsManager.get
    assert(instance.isDefined)
    assert(instance.get.isRegistered)

    MetricsManager.shutdown()
    assert(MetricsManager.get.isEmpty)

    // Recover
    spark.conf.set("spark.pista.metrics.enabled", "false")
  }

  test("MetricsManager.shutdown MetricsManager.shutdown should not throw an exception when there are no instances.") {
    MetricsManager.shutdown()
    MetricsManager.shutdown() // Repeat shutdown
  }

  // ==================== MetricsConfig Testing ====================

  test("MetricsConfig should be correctly constructed") {
    val config = defaultConfig
    assert(config.enabled)
    assert(config.queueCapacity == 100)
    assert(config.dropPolicy == DropPolicy.DropOldest)
    assert(config.batchSize == 10)
  }

  test("register output directory should be created") {
    val manager = new MetricsManager(spark, defaultConfig)
    manager.register()

    assert(new File(outputDir).exists(), "Output directory should be created")

    manager.close()
  }
}
