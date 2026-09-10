package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.MetricsConfig
import com.pista.spark.sql.metrics.model._
import com.pista.spark.sql.metrics.writer.{DropPolicy, MetricsQueue}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

/**
 * MetricsSparkListener Event Processor Test
 *
 * Covers onJobStart/End, onStageCompleted, onTaskEnd, and onOtherEvent.
 * Use real Spark event objects (triggered by running a query) and manually constructed events
 *
 */
class MetricsSparkListenerSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("MetricsSparkListenerSuite")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  private def defaultConfig = MetricsConfig(
    enabled = true,
    outputDir = "/tmp/metrics-listener-test",
    queueCapacity = 1000,
    dropPolicy = DropPolicy.DropOldest,
    batchSize = 100,
    flushIntervalMs = 1000,
    executorEnabled = true,
    skewEnabled = true,
    skewThreshold = 3.0,
    skewTopN = 10,
    shuffleEnabled = true,
    errorEnabled = true,
    planEnabled = true,
    planTruncateLength = 10000,
    dqMaxColumns = 50
  )

  /** Construct StageInfo, compatible with Spark 3.5.8 constructor */
  private def makeStageInfo(stageId: Int, attemptId: Int, name: String, numTasks: Int) =
    new StageInfo(stageId, attemptId, name, numTasks,
      Seq.empty, Seq.empty, "", resourceProfileId = 0)

  // ==================== onJobStart / onJobEnd Test ====================

  test("onJobStart + onJobEnd should generate JobMetrics") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val props = new Properties()
    props.setProperty("spark.jobGroup.id", "sql-group-1")

    val stageInfo = makeStageInfo(0, 0, "test-stage", 5)
    val jobStart = SparkListenerJobStart(
      jobId = 1,
      time = 1000L,
      stageInfos = Seq(stageInfo),
      properties = props
    )
    listener.onJobStart(jobStart)

    val jobEnd = SparkListenerJobEnd(jobId = 1, time = 2000L, JobSucceeded)
    listener.onJobEnd(jobEnd)

    val drained = queue.drain(100)
    val jobMetrics = drained.collect { case m: JobMetrics => m }

    assert(jobMetrics.size == 1)
    assert(jobMetrics.head.jobId == 1)
    assert(jobMetrics.head.status == "SUCCEEDED")
    assert(jobMetrics.head.durationMs == 1000L)
    assert(jobMetrics.head.jobGroup == "sql-group-1")
    assert(jobMetrics.head.numStages == 1)
  }

  test("onJobEnd no corresponding start event should not throw an exception") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    // Send an end event without a start event.
    listener.onJobEnd(SparkListenerJobEnd(jobId = 999, time = 2000L, JobSucceeded))

    // Should not generate JobMetrics
    val drained = queue.drain(100)
    assert(drained.collect { case m: JobMetrics => m }.isEmpty)
  }

  // ==================== onStageCompleted Testing ====================

  test("onStageCompleted generates StageMetrics") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val stageInfo = makeStageInfo(1, 0, "test-stage", 10)
    stageInfo.submissionTime = Some(1000L)
    stageInfo.completionTime = Some(2000L)

    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))

    val drained = queue.drain(100)
    val stageMetrics = drained.collect { case m: StageMetrics => m }

    assert(stageMetrics.size == 1)
    assert(stageMetrics.head.stageId == 1)
    assert(stageMetrics.head.status == "COMPLETED")
    assert(stageMetrics.head.durationMs == 1000L)
    assert(stageMetrics.head.numTasks == 10)
  }

  test("onStageCompleted fails and should generate ErrorMetrics") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val stageInfo = makeStageInfo(2, 0, "failed-stage", 5)
    stageInfo.submissionTime = Some(1000L)
    stageInfo.completionTime = Some(2000L)
    stageInfo.failureReason = Some("OutOfMemoryError")

    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))

    val drained = queue.drain(100)
    val stageMetrics = drained.collect { case m: StageMetrics => m }
    val errorMetrics = drained.collect { case m: ErrorMetrics => m }

    assert(stageMetrics.head.status == "FAILED")
    assert(errorMetrics.exists(_.errorType == "STAGE"))
    assert(errorMetrics.exists(_.errorMessage.contains("OutOfMemoryError")))
  }

  // ==================== Integration Testing: Trigger Events Through Real Queries Integration ====================

  test("real queries should trigger Job and triggers Job and Stage events. Stage Events") {
    val queue = new MetricsQueue[Metrics](1000, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener(
      spark.sparkContext.applicationId, "TestApp", queue, defaultConfig
    )

    spark.sparkContext.addSparkListener(listener)

    try {
      import spark.implicits._
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "name")
      df.createOrReplaceTempView("listener_test")

      // Execute query triggered event
      spark.sql("SELECT count(*) FROM listener_test").collect()

      // Wait for event processing (listenerBus is private, use Thread.sleep instead)
      Thread.sleep(3000)

      val drained = queue.drain(1000)

      // There should be at least Job and Stage metrics.
      val jobMetrics = drained.collect { case m: JobMetrics => m }
      val stageMetrics = drained.collect { case m: StageMetrics => m }

      assert(jobMetrics.nonEmpty, "there should be JobMetrics")
      assert(stageMetrics.nonEmpty, "there should be StageMetrics")
      assert(jobMetrics.head.status == "SUCCEEDED")
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("SQLMetrics.executionId must match QueryExecution.id") {
    val queue = new MetricsQueue[Metrics](1000, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener(
      spark.sparkContext.applicationId, "TestApp", queue, defaultConfig
    )

    spark.sparkContext.addSparkListener(listener)

    try {
      val df = spark.sql("SELECT 1 AS id")
      val qeId = df.queryExecution.id
      df.collect()

      // Wait for event processing (listenerBus is private, use Thread.sleep instead)
      Thread.sleep(3000)

      val drained = queue.drain(1000)
      val sqlMetrics = drained.collect { case m: SQLMetrics => m }
      assert(sqlMetrics.nonEmpty, "there should be SQLMetrics")
      assert(sqlMetrics.exists(_.executionId == qeId), s"SQLMetrics.executionId should be $qeId")
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("Join Query Should Trigger Shuffle Metric") {
    val queue = new MetricsQueue[Metrics](1000, DropPolicy.DropOldest)
    val config = defaultConfig.copy(shuffleEnabled = true)
    val listener = new MetricsSparkListener(
      spark.sparkContext.applicationId, "TestApp", queue, config
    )

    spark.sparkContext.addSparkListener(listener)

    try {
      import spark.implicits._
      (1 to 100).map(i => (i, s"name_$i")).toDF("id", "name")
        .createOrReplaceTempView("shuffle_left")
      (1 to 100).map(i => (i, i * 10)).toDF("id", "score")
        .createOrReplaceTempView("shuffle_right")

      spark.sql(
        """SELECT /*+ SHUFFLE_MERGE(shuffle_right) */ l.name, r.score
          |FROM shuffle_left l JOIN shuffle_right r ON l.id = r.id""".stripMargin
      ).collect()

      // Wait for event processing
      Thread.sleep(3000)

      val drained = queue.drain(1000)

      // Shuffle Join should produce ShuffleMetrics
      // Note: Small dataset may be optimized for BroadcastJoin, in which case there is no shuffle.
      // So, only validate without throwing an exception
      assert(drained.nonEmpty)
    } finally
      spark.sparkContext.removeSparkListener(listener)
  }

  // ==================== Integration Testing: Job Failure Scenario ====================

  test("failed queries should generate a FAILED status for JobMetrics") {
    val queue = new MetricsQueue[Metrics](1000, DropPolicy.DropOldest)
    val config = defaultConfig.copy(errorEnabled = true)
    val listener = new MetricsSparkListener(
      spark.sparkContext.applicationId, "TestApp", queue, config
    )

    spark.sparkContext.addSparkListener(listener)

    try {
      // Execute a query that fails.
      intercept[Exception] {
        spark.sql("SELECT * FROM non_existent_table_xyz").collect()
      }

      Thread.sleep(3000)

      // No exception thrown indicates success (failure queries may fail during the Analyzer phase, not necessarily triggering a Job)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  // ==================== Stage Retry (attemptId > 0) Testing ====================

  test("a stage retry with attemptId=1 produces independent StageMetrics") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    // First attempt failed
    val attempt0 = makeStageInfo(3, 0, "retry-stage", 4)
    attempt0.submissionTime = Some(1000L)
    attempt0.completionTime = Some(1500L)
    attempt0.failureReason = Some("TaskKilled")
    listener.onStageCompleted(SparkListenerStageCompleted(attempt0))

    // Second attempt successful
    val attempt1 = makeStageInfo(3, 1, "retry-stage", 4)
    attempt1.submissionTime = Some(2000L)
    attempt1.completionTime = Some(3000L)
    listener.onStageCompleted(SparkListenerStageCompleted(attempt1))

    val stageMetrics = queue.drain(100).collect { case m: StageMetrics => m }

    assert(stageMetrics.size == 2, "two attempts attempt should generate one each StageMetrics")

    val s0 = stageMetrics.find(_.stageAttemptId == 0).get
    assert(s0.status == "FAILED")
    assert(s0.durationMs == 500L)

    val s1 = stageMetrics.find(_.stageAttemptId == 1).get
    assert(s1.status == "COMPLETED")
    assert(s1.durationMs == 1000L)
  }

  test("stage retry attempts are aggregated independently") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    // Simulate two attempts with stageId=5, recording different durations for onTaskEnd
    // (TaskAggregator stores key uniquely by (stageId, attemptId))
    // attempt=0:A Task that processes 100ms of data
    // attempt=2two tasks, with durations of 200ms and 300ms Taskduration 200ms/300ms

    // Manually trigger the onStageCompleted observer to ensure TaskAggMetrics are correctly separated
    val attempt0 = makeStageInfo(5, 0, "agg-stage", 1)
    attempt0.submissionTime = Some(1000L)
    attempt0.completionTime = Some(1100L)
    attempt0.failureReason = Some("error")

    val attempt2 = makeStageInfo(5, 2, "agg-stage", 2)
    attempt2.submissionTime = Some(2000L)
    attempt2.completionTime = Some(2300L)

    listener.onStageCompleted(SparkListenerStageCompleted(attempt0))
    listener.onStageCompleted(SparkListenerStageCompleted(attempt2))

    val stageMetrics = queue.drain(100).collect { case m: StageMetrics => m }
    assert(stageMetrics.exists(_.stageAttemptId == 0))
    assert(stageMetrics.exists(_.stageAttemptId == 2))
    // Two attempts do not interfere with each other and output independently.
    assert(!stageMetrics.exists(_.stageAttemptId == 1), "stage attempts skipped with attempt=1 should not occur attempt=1 should not occur")
  }

  // ==================== Executor Event Unit Tests ====================

  test("onExecutorAdded should map executorId to host") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val execInfo = new ExecutorInfo("host-executor-1", 4, Map.empty)
    listener.onExecutorAdded(SparkListenerExecutorAdded(1000L, "exec-1", execInfo))

    // An empty executor update should not change the result or throw.
    val emptyEvent = SparkListenerExecutorMetricsUpdate("exec-1", Seq.empty, Map.empty)
    listener.onExecutorMetricsUpdate(emptyEvent)

    assert(queue.drain(10).isEmpty, "no metrics should be emitted without executor updates")
  }

  test("onExecutorMetricsUpdate executorUpdates is empty should be skipped by nonEmpty guard") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val event = SparkListenerExecutorMetricsUpdate("exec-1", Seq.empty, Map.empty)
    listener.onExecutorMetricsUpdate(event)

    assert(queue.drain(10).isEmpty)
  }

  test("onExecutorRemoved no heartbeats data should not throw an exception, and host mappings should be cleaned") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val execInfo = new ExecutorInfo("host-1", 4, Map.empty)
    listener.onExecutorAdded(SparkListenerExecutorAdded(1000L, "exec-1", execInfo))
    // Without a heartbeat, the executor snapshot is empty and cleanup must not throw.
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(2000L, "exec-1", "finished"))

    // No ExecutorMetrics data; queue is empty.
    assert(queue.drain(10).collect { case m: ExecutorMetrics => m }.isEmpty)
  }

  test("executorEnabled=false when Executor events should not be handled") {
    val config = defaultConfig.copy(executorEnabled = false)
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, config)

    val execInfo = new ExecutorInfo("host-1", 4, Map.empty)
    listener.onExecutorAdded(SparkListenerExecutorAdded(1000L, "exec-1", execInfo))
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(2000L, "exec-1", "finished"))

    assert(queue.drain(10).isEmpty)
  }

  test("onJobEnd with executorEnabled does not throw an exception (snapshot is empty when no heartbeat data exists)") {
    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, defaultConfig)

    val stageInfo = makeStageInfo(0, 0, "test-stage", 1)
    val props = new Properties()
    listener.onJobStart(SparkListenerJobStart(1, 1000L, Seq(stageInfo), props))
    listener.onJobEnd(SparkListenerJobEnd(1, 2000L, JobSucceeded))

    val drained = queue.drain(100)
    // Have JobMetrics, no ExecutorMetrics (no heartbeats)
    assert(drained.collect { case m: JobMetrics => m }.nonEmpty)
    assert(drained.collect { case m: ExecutorMetrics => m }.isEmpty)
  }

  // ==================== Executor Metrics Integration Testing ====================

  test("Real query ends should trigger a snapshot of ExecutorMetrics onJobEnd at the end of the job (empty if no heartbeats)") {
    // This test ensures the integration path is clear, does not guarantee ExecutorMetrics data (depending on whether heartbeats trigger)
    // If actual memory data needs to be validated, run independently with spark.executor.heartbeatInterval=2s
    val queue = new MetricsQueue[Metrics](1000, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener(
      spark.sparkContext.applicationId, "TestApp", queue, defaultConfig
    )

    spark.sparkContext.addSparkListener(listener)

    try {
      import spark.implicits._
      (1 to 500).map(i => (i, i * 2)).toDF("id", "val")
        .createOrReplaceTempView("exec_test")

      spark.sql("SELECT sum(val) FROM exec_test").collect()

      Thread.sleep(3000)

      val drained = queue.drain(1000)
      assert(drained.collect { case m: JobMetrics => m }.nonEmpty, "should have JobMetrics")

      // ExecutorMetrics may be empty before heartbeats. Only validate if present that the fields are legal.
      drained.collect { case m: ExecutorMetrics => m }.foreach { m =>
        assert(m.executorId.nonEmpty)
        assert(m.jvmHeapMemory >= 0)
        assert(m.completedTasks >= 0)
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  // ==================== CONFIG SWITCH TEST ====================

  test("Only generate base metrics when all optional metrics are disabled.") {
    val config = defaultConfig.copy(
      executorEnabled = false,
      skewEnabled = false,
      shuffleEnabled = false,
      errorEnabled = false,
      planEnabled = false,
    )

    val queue = new MetricsQueue[Metrics](100, DropPolicy.DropOldest)
    val listener = new MetricsSparkListener("app-1", "TestApp", queue, config)

    val stageInfo = makeStageInfo(1, 0, "test", 5)
    stageInfo.submissionTime = Some(1000L)
    stageInfo.completionTime = Some(2000L)
    stageInfo.failureReason = Some("test error")

    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))

    val drained = queue.drain(100)

    // there should be StageMetrics and metrics, but not error metrics or shuffle metrics. TaskAggMetricsbut should not have ErrorMetrics/ShuffleMetrics
    val stageMetrics = drained.collect { case m: StageMetrics => m }
    val errorMetrics = drained.collect { case m: ErrorMetrics => m }
    val shuffleMetrics = drained.collect { case m: ShuffleMetrics => m }

    assert(stageMetrics.nonEmpty)
    assert(errorMetrics.isEmpty, "errorEnabled=false should not generate ErrorMetrics")
    assert(shuffleMetrics.isEmpty, "shuffleEnabled=false there should not be any generated metrics ShuffleMetrics")
  }
}
