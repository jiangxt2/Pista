package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.sql.connector.BatchWriteResult
import com.pista.spark.sql.doris.meta.DorisMetaManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 *DorisConcurrentWriter Unit Test*
 *
 * Test content:
 * - resolvePartitionFilter automatically infers the filter rule
 * - taskId generation rule (including clusterName / inference filter)
 * - Idempotent skip (return directly if taskId has succeeded)
 * - Meta Optional Degradation (Write Normally When No Meta)
 */
class DorisConcurrentWriterSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DorisConcurrentWriterSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ==================== resolvePartitionFilter ====================

  test("resolvePartitionFilter: Automatically infer single date value") {
    val config = minimalConfig().copy(
      partitionColumn = "partition_date",
      partitionDate = "20260407"
    )
    assert(config.resolvePartitionFilter().contains("partition_date = '20260407'"))
  }

  test("resolvePartitionFilter: infer multiple date values automatically") {
    val config = minimalConfig().copy(
      partitionColumn = "partition_date",
      partitionDate = "20260407,20260408"
    )
    assert(config.resolvePartitionFilter().contains("partition_date IN ('20260407', '20260408')"))
  }

  test("resolvePartitionFilter: Multiple date values with spaces automatically infer") {
    val config = minimalConfig().copy(
      partitionColumn = "partition_date",
      partitionDate = "20260407, 20260408"
    )
    assert(config.resolvePartitionFilter().contains("partition_date IN ('20260407', '20260408')"))
  }

  test("resolvePartitionFilter: No column or date returns None") {
    val config1 = minimalConfig().copy(partitionColumn = "", partitionDate = "20260407")
    assert(config1.resolvePartitionFilter().isEmpty)

    val config2 = minimalConfig().copy(partitionColumn = "partition_date", partitionDate = "")
    assert(config2.resolvePartitionFilter().isEmpty)
  }

  // ==================== taskId generation rule ====================

  test("taskId without partition conditions has suffix all") {
    val config = minimalConfig()
    val writer = new TestableDorisConcurrentWriter(config, noMetaContext, spark)
    val taskId = writer.exposedBuildTaskId(config, Some("test_cluster"))
    assert(taskId == "test_cluster_pista_mydb_mytable_all")
  }

  test("taskId using inference-based filter suffix") {
    val config = minimalConfig().copy(
      partitionColumn = "partition_date",
      partitionDate = "20260407"
    )
    val writer = new TestableDorisConcurrentWriter(config, noMetaContext, spark)
    val taskId = writer.exposedBuildTaskId(config, Some("test_cluster"))
    assert(taskId == "test_cluster_pista_mydb_mytable_partition_date = '20260407'")
  }

  test("taskId clusterName is None when using default_cluster") {
    val config = minimalConfig()
    val writer = new TestableDorisConcurrentWriter(config, noMetaContext, spark)
    val taskId = writer.exposedBuildTaskId(config, None)
    assert(taskId.startsWith("default_cluster_"))
  }

  test("taskId long inference filter degrades to SHA-256 upon long task inference pause SHA-256") {
    val longDate = "20260407," * 300 + "20260408"  // ~3600+ bytes, exceeds 2600 limit
    val config = minimalConfig().copy(
      partitionColumn = "partition_date",
      partitionDate = longDate
    )
    val writer = new TestableDorisConcurrentWriter(config, noMetaContext, spark)
    val taskId = writer.exposedBuildTaskId(config, Some("test_cluster"))

    assert(!taskId.contains(longDate))
    val suffix = taskId.stripPrefix("test_cluster_pista_mydb_mytable_")
    assert(suffix.length == 64)
  }

  // ==================== Idempotent Skip ====================

  test("write returns rowCount=0 and success=true when Meta already marks the task successful") {
    val config = minimalConfig()
    val mockMeta = new AlwaysSucceededMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(mockMeta)
    )
    val df = spark.range(10).toDF("id")
    val writer = new DorisConcurrentWriter(config, context, df, System.currentTimeMillis())
    val result = writer.write()
    assert(result.success)
    assert(result.rowCount == 0L)
  }

  // ==================== Meta Optional Rollback ====================

  test("context construction succeeds without a metadata manager") {
    val ctx = DorisConcurrentContext(clusterName = Some("test_cluster"), metaManager = None)
    assert(ctx.metaManager.isEmpty)
    assert(ctx.clusterName.contains("test_cluster"))
  }

  test("overwrite=true forces a write even when metadata reports success") {
    val config = minimalConfig().copy(overwrite = true)
    val mockMeta = new AlwaysSucceededMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(mockMeta)
    )
    val df = spark.range(10).toDF("id")
    val writer = new DorisConcurrentWriter(config, context, df, System.currentTimeMillis())
    val ex = intercept[Exception] { writer.write() }
    assert(!ex.getMessage.contains("already succeeded"))
  }

  // ==================== abortAndCleanup Cleanup Path ====================

  test("Connector fails to write triggers abortAndCleanup") {
    val meta = new RecordingMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(meta)
    )
    val config = minimalConfig().copy(enable2PC = true)
    val writer = new TestableDorisConcurrentWriter(config, context, spark)
    writer.connectorWriteException = Some(new RuntimeException("connector error"))
    writer.abortByLabelResult = 2

    val ex = intercept[Exception] { writer.write() }
    assert(ex.getMessage.contains("connector error"))
    assert(writer.abortByLabelCallCount == 2) // Phase 2 cleanup and abortAndCleanup rollback
    assert(meta.failureMessage.isDefined)
  }

  test("ALTER TABLE REPLACE fails and triggers abortAndCleanup") {
    val meta = new RecordingMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(meta)
    )
    val config = minimalConfig().copy(overwrite = true, enable2PC = true)
    val writer = new TestableDorisConcurrentWriter(config, context, spark)
    writer.replaceTableException = Some(new RuntimeException("replace failed"))
    writer.abortByLabelResult = 1

    val ex = intercept[Exception] { writer.write() }
    assert(ex.getMessage.contains("replace failed"))
    assert(writer.cleanupTempTableCalled)
    assert(writer.abortByLabelCallCount == 2) // Phase 2 cleanup and abortAndCleanup rollback
    assert(meta.failureMessage.isDefined)
  }

  test("REPLACE PARTITION fails and triggers abortAndCleanup") {
    val meta = new RecordingMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(meta)
    )
    val config = minimalConfig().copy(
      overwrite = true,
      partitionDate = "20260407",
      partitionColumn = "partition_date",
      enable2PC = true)
    val writer = new TestableDorisConcurrentWriter(config, context, spark)
    writer.replacePartitionsException = Some(new RuntimeException("replace partitions failed"))
    writer.abortByLabelResult = 1

    val ex = intercept[Exception] { writer.write() }
    assert(ex.getMessage.contains("replace partitions failed"))
    assert(writer.cleanupTempPartitionsCalled)
    assert(!writer.cleanupTempTableCalled)
    assert(writer.abortByLabelCallCount == 2) // Phase 2 cleanup and abortAndCleanup rollback
    assert(meta.failureMessage.isDefined)
  }

  // ==================== 2PC Timeout Path Verification ====================

  test("2PC validation called abortByLabel when timeout and PRECOMMITTED exist") {
    val meta = new RecordingMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(meta)
    )
    val config = minimalConfig().copy(enable2PC = true)
    val writer = new TestableDorisConcurrentWriter(config, context, spark)
    writer.validateAllVisibleResult = false
    writer.transactionStatuses = Seq("PRECOMMITTED")
    writer.abortByLabelResult = 1

    val ex = intercept[Exception] { writer.write() }
    assert(ex.getMessage.contains("not all transactions visible within timeout"))
    assert(writer.abortByLabelCallCount == 2) // Phase 2 Cleanup + Phase 6 Timeout Rollback
    assert(meta.failureMessage.isDefined)
  }

  test("2PC validation times out but all are COMMITTED without invoking abortByLabel") {
    val meta = new RecordingMetaManager()
    val context = DorisConcurrentContext(
      clusterName = Some("test_cluster"),
      metaManager = Some(meta)
    )
    val config = minimalConfig().copy(enable2PC = true)
    val writer = new TestableDorisConcurrentWriter(config, context, spark)
    writer.validateAllVisibleResult = false
    writer.transactionStatuses = Seq("COMMITTED")

    val ex = intercept[Exception] { writer.write() }
    assert(ex.getMessage.contains("not all transactions visible within timeout"))
    assert(writer.abortByLabelCallCount == 1) // Only Phase 2 cleanup, no additional abort in Phase 6
    assert(meta.failureMessage.isDefined)
  }

  // ==================== Helper Methods ====================

  private def minimalConfig() = DorisBatchConfig(
    fenodes  = "fe1:8030",
    database = "mydb",
    table    = "mytable",
    password = ""
  )

  private def noMetaContext = DorisConcurrentContext(
    clusterName = Some("test_cluster"),
    metaManager = None
  )
}

/** Mock meta manager that always reports `isSucceeded=true`.
 *
 * Passing null to the parent constructor is safe here because DorisMetaManager only assigns
 * those fields during construction. This class overrides every method and never accesses them.
 */
class AlwaysSucceededMetaManager extends DorisMetaManager(null, null) {
  override def isSucceeded(taskId: String): Boolean = true
  override def hasAnyRecord(taskId: String): Boolean = true
  override def upsertRunning(
    taskId: String, database: String, table: String,
    partitionDate: String, writeMode: String,
    sourceTable: Option[String], partitionFilter: Option[String]
  ): Unit = ()
  override def markSuccess(taskId: String, rowCount: Long): Unit = ()
  override def markFailure(taskId: String, errorMessage: String): Unit = ()
  override def close(): Unit = ()
}

/** Record markFailure / markSuccess calls to Mock MetaManager */
class RecordingMetaManager extends DorisMetaManager(null, null) {
  var failureMessage: Option[String] = None
  var successRowCount: Option[Long] = None

  override def isSucceeded(taskId: String): Boolean = false
  override def hasAnyRecord(taskId: String): Boolean = false
  override def upsertRunning(
    taskId: String, database: String, table: String,
    partitionDate: String, writeMode: String,
    sourceTable: Option[String], partitionFilter: Option[String]
  ): Unit = ()
  override def markSuccess(taskId: String, rowCount: Long): Unit = {
    successRowCount = Some(rowCount)
  }
  override def markFailure(taskId: String, errorMessage: String): Unit = {
    failureMessage = Some(errorMessage)
  }
  override def close(): Unit = ()
}

/**
 * Testable DorisConcurrentWriter subclass
 *
 * Expose protected buildTaskId to tests and override all factory methods to support mocking.
 */
class TestableDorisConcurrentWriter(
  batchConfig: DorisBatchConfig,
  context:     DorisConcurrentContext,
  spark:       SparkSession
) extends DorisConcurrentWriter(
  batchConfig,
  context,
  spark.range(1).toDF("id"),
  System.currentTimeMillis()
) {
  var abortByLabelCallCount: Int = 0
  var cleanupTempTableCalled: Boolean = false
  var cleanupTempPartitionsCalled: Boolean = false

  var connectorWriteResult: BatchWriteResult = BatchWriteResult(0, 0L, success = true)
  var connectorWriteException: Option[Exception] = None

  var validateAllVisibleResult: Boolean = true
  var transactionStatuses: Seq[String] = Seq.empty
  var abortByLabelResult: Int = 0

  var replaceTableException: Option[Exception] = None
  var replacePartitionsException: Option[Exception] = None

  override protected def createConnectorWriter(
    config: DorisBatchConfig, df: DataFrame, ts: Long
  ): SparkDorisConnectorWriter = new SparkDorisConnectorWriter(config, df, ts) {
    override def write(): BatchWriteResult = connectorWriteException match {
      case Some(ex) => throw ex
      case None     => connectorWriteResult
    }
  }

  override protected def createTransactionCoordinator(): DorisTransactionCoordinator = new DorisTransactionCoordinator() {
    override def validateAllVisible(config: DorisBatchConfig, maxWaitMs: Int): Boolean = validateAllVisibleResult
    override def queryTransactionStatuses(config: DorisBatchConfig): Seq[String] = transactionStatuses
    override def abortByLabel(config: DorisBatchConfig): Int = {
      abortByLabelCallCount += 1
      abortByLabelResult
    }
  }

  override protected def createTableSwapManager(
    config: DorisBatchConfig, ts: Long
  ): DorisTableSwapManager = new DorisTableSwapManager(config, ts) {
    override def createTempTable(): Unit = ()
    override def replaceTable(): Unit = replaceTableException.foreach(throw _)
    override def cleanupTempTable(): Unit = cleanupTempTableCalled = true
  }

  override protected def createPartitionManager(
    config: DorisBatchConfig, ts: Long
  ): DorisPartitionManager = new DorisPartitionManager(config, ts) {
    override def prepareTempPartitions(): Unit = ()
    override def replacePartitions(): Unit = replacePartitionsException.foreach(throw _)
    override def cleanupTempPartitions(): Unit = cleanupTempPartitionsCalled = true
  }

  def exposedBuildTaskId(config: DorisBatchConfig, clusterName: Option[String]): String =
    buildTaskId(config, clusterName)
}
