package com.pista.spark.sql.metrics.collector

import com.pista.spark.sql.metrics.MetricsConfig
import com.pista.spark.sql.metrics.model._
import com.pista.spark.sql.metrics.writer.MetricsQueue
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

/**
 * Spark Metrics Listener
 *
 * listen for Spark events and collect job/stage/query/task metrics. Spark listeners, collect Job/Stage/SQL/Task metrics.
 * All callbacks only enqueue operations and do not perform IO to avoid blocking the event bus.
 *
 * @param appId application ID
 * @param appName Application Name
 * @param queue   Metric Queue
 * @param config  Metric Configuration
 *
 */
class MetricsSparkListener(
  appId: String,
  appName: String,
  queue: MetricsQueue[Metrics],
  config: MetricsConfig
) extends SparkListener with Logging {

  // Job Start Information: jobId -> (startTime, jobGroup, numStages, numTasks)
  private val jobStartInfo = new ConcurrentHashMap[Int, (Long, String, Int, Int)]()

  // Stage -> Job Mapping: stageId -> jobId
  private val stageToJobId = new ConcurrentHashMap[Int, Int]()

  // SQL Execution Start Information: (executionId, (startTimestamp, sqlText))
  private val sqlStartInfo = new ConcurrentHashMap[Long, (Long, String)]()
  // SQLExecution.executionId -> QueryExecution.id (used to unify the executionId semantics)
  private val sqlToQeId = new ConcurrentHashMap[Long, Long]()

  // Task aggregator
  private val taskAggregator = new TaskAggregator(config.skewThreshold, config.skewTopN)

  // Executor Aggregator (if enabled)
  private val executorAggregator = if (config.executorEnabled) new ExecutorAggregator() else null

  // executorId -> host mapping (maintained by onExecutorAdded and used by onExecutorMetricsUpdate)
  private val executorHosts = new ConcurrentHashMap[String, String]()

  // Query plan extractor (if enabled)
  private val queryPlanExtractor = if (config.planEnabled) new QueryPlanExtractor(config.planTruncateLength) else null

  // Query execution metrics checkpoint: executionId -> QueryPlanMetrics
  private val planMetricsCache = new ConcurrentHashMap[Long, QueryPlanMetrics]()

  // ==================== Job Event ====================

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty("spark.jobGroup.id")))
      .getOrElse("")

    val numStages = jobStart.stageIds.size
    val numTasks = jobStart.stageInfos.map(_.numTasks).sum

    // Record job start information.
    jobStartInfo.put(jobStart.jobId, (jobStart.time, jobGroup, numStages, numTasks))

    // establish the mapping from Stage to Job Stage -> Job mapping
    jobStart.stageIds.foreach(stageId => stageToJobId.put(stageId, jobStart.jobId))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val info = jobStartInfo.remove(jobEnd.jobId)
    if (info == null) {
      logWarning(s"Job ${jobEnd.jobId} end without start event")
      return
    }

    val (startTime, jobGroup, numStages, numTasks) = info
    val endTime = jobEnd.time
    val status = if (jobEnd.jobResult == JobSucceeded) "SUCCEEDED" else "FAILED"

    val metrics = JobMetrics(
      appId = appId,
      appName = appName,
      jobId = jobEnd.jobId,
      jobGroup = jobGroup,
      status = status,
      startTime = startTime,
      endTime = endTime,
      durationMs = endTime - startTime,
      numStages = numStages,
      numTasks = numTasks,
      collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
    )

    queue.offer(metrics)

    // Handle Job Failures (ErrorMetrics)
    if (config.errorEnabled && jobEnd.jobResult != JobSucceeded) {
      val errorMessage = jobEnd.jobResult.toString

      val errorMetrics = ErrorMetrics(
        appId = appId,
        errorType = "JOB",
        executionId = None,
        sqlHash = None,
        jobId = Some(jobEnd.jobId),
        stageId = None,
        stageAttemptId = None,
        taskId = None,
        taskAttemptId = None,
        errorMessage = errorMessage,
        errorClass = None,
        stackTrace = None,
        executorId = None,
        host = None,
        failureReason = "JOB_FAILED",
        retryCount = 0,
        failureTime = endTime,
        collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
      )
      queue.offer(errorMetrics)
    }

    // Output snapshot of active Executor resources (dynamic allocation deallocation recycled in onExecutorRemoved)
    if (config.executorEnabled && executorAggregator != null)
      executorAggregator.snapshot(appId).foreach(queue.offer)
  }

  // ==================== Stage event ====================

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val tm = Option(info.taskMetrics)

    // Retrieve jobId from the map (AQE dynamically added Stage may be -1)
    val jobId = stageToJobId.getOrDefault(info.stageId, -1)

    val status = if (info.failureReason.isDefined) "FAILED" else "COMPLETED"
    val startTime = info.submissionTime.getOrElse(0L)
    val endTime = info.completionTime.getOrElse(0L)

    val metrics = StageMetrics(
      appId = appId,
      stageId = info.stageId,
      stageAttemptId = info.attemptNumber(),
      jobId = jobId,
      status = status,
      startTime = startTime,
      endTime = endTime,
      durationMs = endTime - startTime,
      numTasks = info.numTasks,
      inputBytes = tm.map(_.inputMetrics.bytesRead).getOrElse(0L),
      inputRecords = tm.map(_.inputMetrics.recordsRead).getOrElse(0L),
      outputBytes = tm.map(_.outputMetrics.bytesWritten).getOrElse(0L),
      outputRecords = tm.map(_.outputMetrics.recordsWritten).getOrElse(0L),
      shuffleReadBytes = tm.map(_.shuffleReadMetrics.totalBytesRead).getOrElse(0L),
      shuffleWriteBytes = tm.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(0L),
      executorRunTime = tm.map(_.executorRunTime).getOrElse(0L),
      executorCpuTime = tm.map(_.executorCpuTime).getOrElse(0L),
      jvmGcTime = tm.map(_.jvmGCTime).getOrElse(0L),
      collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
    )

    queue.offer(metrics)

    // Output aggregated metrics for Task shards
    taskAggregator.aggregateAndClear(info.stageId, info.attemptNumber(), appId)
      .foreach(queue.offer)

    // Output data skew metrics (if enabled)
    if (config.skewEnabled) {
      taskAggregator.aggregateSkewMetrics(info.stageId, info.attemptNumber(), appId)
        .foreach(queue.offer)
    }

    // Output Shuffle Metrics (if enabled)
    if (config.shuffleEnabled) {
      tm.foreach { taskMetrics =>
        val shuffleMetrics = ShuffleMetrics(
          appId = appId,
          stageId = info.stageId,
          stageAttemptId = info.attemptNumber(),
          shuffleWriteBytes = taskMetrics.shuffleWriteMetrics.bytesWritten,
          shuffleWriteRecords = taskMetrics.shuffleWriteMetrics.recordsWritten,
          shuffleWriteTimeNs = taskMetrics.shuffleWriteMetrics.writeTime,
          shuffleReadBytes = taskMetrics.shuffleReadMetrics.totalBytesRead,
          shuffleReadRecords = taskMetrics.shuffleReadMetrics.recordsRead,
          shuffleFetchWaitTimeMs = taskMetrics.shuffleReadMetrics.fetchWaitTime,
          shuffleRemoteBlocksFetched = taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
          shuffleLocalBlocksFetched = taskMetrics.shuffleReadMetrics.localBlocksFetched,
          shuffleSpillMemory = taskMetrics.memoryBytesSpilled,
          shuffleSpillDisk = taskMetrics.diskBytesSpilled,
          collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
        )
        queue.offer(shuffleMetrics)
      }
    }

    // Handle Stage Failures (ErrorMetrics)
    if (config.errorEnabled && info.failureReason.isDefined) {
      val errorMetrics = ErrorMetrics(
        appId = appId,
        errorType = "STAGE",
        executionId = None,
        sqlHash = None,
        jobId = Option(stageToJobId.get(info.stageId)).map(_.intValue()),
        stageId = Some(info.stageId),
        stageAttemptId = Some(info.attemptNumber()),
        taskId = None,
        taskAttemptId = None,
        errorMessage = info.failureReason.getOrElse(""),
        errorClass = None,
        stackTrace = None,
        executorId = None,
        host = None,
        failureReason = info.failureReason.getOrElse("STAGE_FAILED"),
        retryCount = info.attemptNumber(),
        failureTime = endTime,
        collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
      )
      queue.offer(errorMetrics)
    }

  }

  // ==================== Task Event ====================

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    if (config.executorEnabled && executorAggregator != null)
      executorAggregator.recordTaskStart(taskStart.taskInfo.executorId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val duration = taskEnd.taskInfo.duration
    val taskInfo = taskEnd.taskInfo
    val taskMetrics = taskEnd.taskMetrics

    // record task execution duration (for TaskAggMetrics) Task execution duration (used for TaskAggMetrics)
    taskAggregator.record(taskEnd.stageId, taskEnd.stageAttemptId, duration)

    // Record detailed Task data (for DataSkewMetrics)
    if (config.skewEnabled && taskMetrics != null) {
      // Estimate partition size: prefer shuffleReadBytes and fall back to inputBytes
      val partitionSize = {
        val shuffleReadBytes = Option(taskMetrics.shuffleReadMetrics)
          .map(_.totalBytesRead).filter(_ > 0)
        val inputBytes = Option(taskMetrics.inputMetrics)
          .map(_.bytesRead).filter(_ > 0)
        shuffleReadBytes.orElse(inputBytes).getOrElse(0L)
      }

      taskAggregator.recordTaskData(
        taskEnd.stageId,
        taskEnd.stageAttemptId,
        taskInfo.partitionId,
        duration,
        partitionSize
      )
    }

    // Record Executor Task Statistics (for ExecutorMetrics)
    if (config.executorEnabled && executorAggregator != null) {
      val success = taskEnd.reason == org.apache.spark.Success
      executorAggregator.recordTaskEnd(taskInfo.executorId, success)
    }

    // Handle Task Failures (ErrorMetrics)
    if (config.errorEnabled && taskEnd.reason != org.apache.spark.Success) {
      taskEnd.reason match {
        case ef: org.apache.spark.ExceptionFailure =>
          val errorMetrics = ErrorMetrics(
            appId = appId,
            errorType = "TASK",
            executionId = None,
            sqlHash = None,
            jobId = Option(stageToJobId.get(taskEnd.stageId)).map(_.intValue()),
            stageId = Some(taskEnd.stageId),
            stageAttemptId = Some(taskEnd.stageAttemptId),
            taskId = Some(taskInfo.taskId),
            taskAttemptId = Some(taskInfo.attemptNumber),
            errorMessage = ef.description,
            errorClass = Some(ef.className),
            stackTrace = Some(ef.stackTrace.mkString("\n")),
            executorId = Some(taskInfo.executorId),
            host = Some(taskInfo.host),
            failureReason = "EXCEPTION_FAILURE",
            retryCount = taskInfo.attemptNumber,
            failureTime = taskInfo.finishTime,
            collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
          )
          queue.offer(errorMetrics)

        case ff: org.apache.spark.FetchFailed =>
          val errorMetrics = ErrorMetrics(
            appId = appId,
            errorType = "TASK",
            executionId = None,
            sqlHash = None,
            jobId = Option(stageToJobId.get(taskEnd.stageId)).map(_.intValue()),
            stageId = Some(taskEnd.stageId),
            stageAttemptId = Some(taskEnd.stageAttemptId),
            taskId = Some(taskInfo.taskId),
            taskAttemptId = Some(taskInfo.attemptNumber),
            errorMessage = ff.toErrorString,
            errorClass = None,
            stackTrace = None,
            executorId = Some(taskInfo.executorId),
            host = Some(taskInfo.host),
            failureReason = "FETCH_FAILED",
            retryCount = taskInfo.attemptNumber,
            failureTime = taskInfo.finishTime,
            collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
          )
          queue.offer(errorMetrics)

        case other =>
          val errorMetrics = ErrorMetrics(
            appId = appId,
            errorType = "TASK",
            executionId = None,
            sqlHash = None,
            jobId = Option(stageToJobId.get(taskEnd.stageId)).map(_.intValue()),
            stageId = Some(taskEnd.stageId),
            stageAttemptId = Some(taskEnd.stageAttemptId),
            taskId = Some(taskInfo.taskId),
            taskAttemptId = Some(taskInfo.attemptNumber),
            errorMessage = other.toString,
            errorClass = None,
            stackTrace = None,
            executorId = Some(taskInfo.executorId),
            host = Some(taskInfo.host),
            failureReason = other.getClass.getSimpleName,
            retryCount = taskInfo.attemptNumber,
            failureTime = taskInfo.finishTime,
            collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
          )
          queue.offer(errorMetrics)
      }
    }
  }

  // ==================== SQL EVENT ====================

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      val sqlText = normalizeSqlText(Option(e.description).getOrElse(""))
      sqlStartInfo.put(e.executionId, (e.time, sqlText))

      // Get QueryExecution (needed for query optimization metrics)
      val qeOpt = try {
        Option(org.apache.spark.sql.execution.SQLExecution.getQueryExecution(e.executionId))
      } catch {
        case ex: Exception =>
          logWarning(s"Failed to get QueryExecution for execution ${e.executionId}: ${ex.getMessage}")
          None
      }

      qeOpt.foreach { qe =>
        sqlToQeId.put(e.executionId, qe.id)
        if (config.planEnabled && queryPlanExtractor != null) {
          try {
            val metrics = queryPlanExtractor.extract(qe, appId, e.executionId, md5(e.description))
            if (planMetricsCache.size() < 1000)
              planMetricsCache.put(e.executionId, metrics)
            else
              logWarning(s"Query plan metrics cache full (${planMetricsCache.size()}), dropping metrics for execution ${e.executionId}")
          } catch {
            case ex: Exception =>
              logWarning(s"Failed to extract query plan metrics for execution ${e.executionId}: ${ex.getMessage}")
          }
        }
      }

    case e: SparkListenerSQLExecutionEnd =>
      Option(sqlStartInfo.remove(e.executionId)).foreach { case (startTimeTs, sqlText) =>
        val endTimeTs = e.time
        val collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
        val durationMs = math.max(0L, endTimeTs - startTimeTs)
        val errorMessageOpt = e.errorMessage.filter(_.nonEmpty)
        val status = if (errorMessageOpt.isDefined) "FAILED" else "SUCCESS"
        val errorMessage = errorMessageOpt.getOrElse("")
        val qeExecutionId = Option(sqlToQeId.remove(e.executionId)).map(_.longValue()).getOrElse(e.executionId)

        val metrics = SQLMetrics(
          appId = appId,
          executionId = qeExecutionId,
          sqlText = sqlText,
          status = status,
          errorMessage = errorMessage,
          startTime = startTimeTs,
          endTime = endTimeTs,
          durationMs = durationMs,
          collectTimestamp = collectTimestamp
        )
        queue.offer(metrics)

        if (config.errorEnabled && errorMessageOpt.isDefined) {
          val errorMetrics = ErrorMetrics(
            appId = appId,
            errorType = "SQL",
            executionId = Some(qeExecutionId),
            sqlHash = Some(md5(sqlText)),
            jobId = None,
            stageId = None,
            stageAttemptId = None,
            taskId = None,
            taskAttemptId = None,
            errorMessage = errorMessage,
            errorClass = None, // SQL level does not have errorClass
            stackTrace = None, // At SQL level, there is no stackTrace
            executorId = None,
            host = None,
            failureReason = "SQL_EXECUTION_FAILED",
            retryCount = 0,
            failureTime = endTimeTs,
            collectTimestamp = Metrics.formatTime(System.currentTimeMillis())
          )
          queue.offer(errorMetrics)
        }
      }

      Option(planMetricsCache.remove(e.executionId)).foreach(queue.offer)

    case _ =>
  }

  // ==================== Executor Event ====================

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit =
    executorHosts.put(executorAdded.executorId, executorAdded.executorInfo.executorHost)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    if (config.executorEnabled && executorAggregator != null) {
      // Snapshot first and clean up: capture the last valid metric during dynamic allocation recycling to avoid data loss at onJobEnd
      executorAggregator.snapshot(appId)
        .filter(_.executorId == executorRemoved.executorId)
        .foreach(queue.offer)
      executorAggregator.clear(executorRemoved.executorId)
    }
    executorHosts.remove(executorRemoved.executorId)
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    if (config.executorEnabled && executorAggregator != null && event.executorUpdates.nonEmpty) {
      val host = executorHosts.getOrDefault(event.execId, "unknown")
      // Take the record with the largest JVMHeapMemory in the peak values of each stage of this heartbeat as a representative value (heuristic approximation)
      val metrics = event.executorUpdates.values.maxBy(_.getMetricValue("JVMHeapMemory"))
      executorAggregator.updateMetrics(event.execId, host, metrics)
    }
  }

  // ==================== Utility Functions ====================

  private def md5(text: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(text.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  /**
   * Remove consecutive newline characters at the beginning of the SQL text while keeping the rest unchanged.
   */
  private def normalizeSqlText(text: String): String =
    text.dropWhile(ch => ch == '\n' || ch == '\r')
}
