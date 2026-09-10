package com.pista.spark.sql.connector.clickhouse.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable

/**
 * ClickHouseConcurrentWriter Task Unit Test
 *
 * Test content:
 * - ensureDataCleared read batchConfig configuration (default / custom / clear immediately)
 */
class ClickHouseConcurrentWriterSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ClickHouseConcurrentWriterSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("ensureDataCleared defaults maxAttempts to 30 and accepts a custom interval") {
    // Validate default values correctness
    val defaultConfig = ClickHouseBatchConfig(
      database = "test_db", table = "test_table", jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db"
    )
    assert(defaultConfig.clearDataMaxAttempts == 30)
    assert(defaultConfig.clearDataIntervalMs == 10000L)

    // Use actual test short interval for acceleration (maintain maxAttempts=30 to verify retry counts)
    val config = defaultConfig.copy(clearDataIntervalMs = 10L)
    val callLog = mutable.ListBuffer.empty[Int]
    val writer = new TestableConcurrentWriter(spark, config, countFn = (_, _, _, _) => {
      callLog += callLog.size
      5L // NEVER CLEAR, TRIGGER FULL POLLING
    })

    val start = System.currentTimeMillis()
    val result = writer.testEnsureDataCleared(
      "jdbc:clickhouse://localhost:8123/test_db", "test_table", "", "", Map.empty
    )
    val elapsed = System.currentTimeMillis() - start

    assert(!result, "should return false (checkpoint not overwritten)")
    assert(callLog.size == 30, s"Default should poll 30 times, actual ${callLog.size}")
    assert(elapsed >= 200 && elapsed < 2000,
      s"30 polls at 10ms should remain between 200ms and 2000ms; actual ${elapsed}ms")
  }

  test("ensureDataCleared Custom Configuration: 3 Times × 50 Milliseconds Quick Timeout") {
    val config = ClickHouseBatchConfig(
      database = "test_db", table = "test_table", jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      clearDataMaxAttempts = 3,
      clearDataIntervalMs = 50L
    )

    val callLog = mutable.ListBuffer.empty[Int]
    val writer = new TestableConcurrentWriter(spark, config, countFn = (_, _, _, _) => {
      callLog += callLog.size
      1L
    })

    val start = System.currentTimeMillis()
    val result = writer.testEnsureDataCleared(
      "jdbc:clickhouse://localhost:8123/test_db", "test_table", "", "", Map.empty
    )
    val elapsed = System.currentTimeMillis() - start

    assert(!result, "should return false (checkpoint not overwritten)")
    assert(callLog.size == 3, s"should converge on 3 transactions, actual ${callLog.size}")
    assert(elapsed >= 100 && elapsed < 500, s"3 polls at 50ms should take at least 100ms and remain below 500ms; actual ${elapsed}ms")
  }

  test("ensureDataCleared Data is immediately cleared, queried only once") {
    val config = ClickHouseBatchConfig(
      database = "test_db", table = "test_table", jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      clearDataMaxAttempts = 10,
      clearDataIntervalMs = 1000L // Although set to a long duration, no waiting should occur
    )

    var callCount = 0
    val writer = new TestableConcurrentWriter(spark, config, countFn = (_, _, _, _) => {
      callCount += 1
      0L // Return 0 immediately upon initial clearing.
    })

    val start = System.currentTimeMillis()
    val result = writer.testEnsureDataCleared(
      "jdbc:clickhouse://localhost:8123/test_db", "test_table", "", "", Map.empty
    )
    val elapsed = System.currentTimeMillis() - start

    assert(result, "should return true (checkpointed)")
    assert(callCount == 1, s"should query only 1 time, actual $callCount")
    assert(elapsed < 200, s"Immediately complete, actual ${elapsed}ms")
  }

  test("ensureDataCleared By Partition Condition Correctly Pass Through") {
    val config = ClickHouseBatchConfig(
      database = "test_db", table = "test_table", jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      clearDataMaxAttempts = 2,
      clearDataIntervalMs = 10L
    )

    var capturedCondition = ""
    val writer = new TestableConcurrentWriter(spark, config, countFn = (_, _, _, condition) => {
      capturedCondition = condition
      0L
    })

    writer.testEnsureDataCleared(
      "jdbc:clickhouse://localhost:8123/test_db", "test_table",
      datePartition = "20260401", partCol = "partition_date", Map.empty
    )

    assert(capturedCondition == "partition_date = '20260401'",
      s"Expected partition_date = '20260401', got '$capturedCondition'")
  }
}

/**
 * Test utility class: expose private methods of ClickHouseConcurrentWriter and mock JDBC calls
 */
class TestableConcurrentWriter(
  override protected val spark: SparkSession,
  config: ClickHouseBatchConfig,
  countFn: (String, String, Map[String, String], String) => Long
) extends ClickHouseConcurrentWriter(
  sourceDf = spark.emptyDataFrame,
  primaryKey = None,
  batchConfig = config,
  context = ClickHouseConcurrentContext(machineCount = 1)
) {

  override protected def clearPartitionViaJdbc(
    jdbcUrl: String, table: String, datePartition: String, options: Map[String, String]
  ): Unit = () // mock: perform no operation

  override protected def countRowsViaJdbc(
    jdbcUrl: String, table: String, options: Map[String, String], conditions: String
  ): Long = countFn(jdbcUrl, table, options, conditions)

  def testEnsureDataCleared(
    jdbcUrl: String, tbl: String, datePartition: String, partCol: String, opts: Map[String, String]
  ): Boolean = {
    val method = classOf[ClickHouseConcurrentWriter].getDeclaredMethod(
      "ensureDataCleared",
      classOf[String], classOf[String], classOf[String], classOf[String], classOf[Map[String, String]]
    )
    method.setAccessible(true)
    method.invoke(this, jdbcUrl, tbl, datePartition, partCol, opts).asInstanceOf[Boolean]
  }
}
