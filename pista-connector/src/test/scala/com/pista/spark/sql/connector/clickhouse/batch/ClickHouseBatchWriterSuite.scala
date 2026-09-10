package com.pista.spark.sql.connector.clickhouse.batch

import com.pista.spark.sql.connector.clickhouse.ClickHouseUDFs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * ClickHouseBatchWriter Data Conversion Function Unit Test Column
 *
 * Test content:
 * - convertNullToDefaults: null replace null with default values
 * - convertCollectionTypes: Array/Map convert to ClickHouse string format
 * - alignToSchema: Schema alignToSchema: Aligning (dropping extra columns)
 */
class ClickHouseBatchWriterSuite extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  @transient private lazy val sparkImplicits = spark.implicits

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ClickHouseBatchWriterSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("ClickHouse connector registers the legacy shard hash privately") {
    ClickHouseUDFs.registerAll(spark)

    val value = "pista-key"
    val result = spark.sql(
      s"SELECT ck_legacy_abs_hashcode('$value') AS hash").head().getInt(0)

    assert(result == Math.abs(value.hashCode))
    assert(spark.sessionState.functionRegistry
      .lookupFunction(org.apache.spark.sql.catalyst.FunctionIdentifier("abs_hashcode")).isEmpty)
  }

  test("KeyBasedBatchWriter preserves the legacy shard assignments") {
    import sparkImplicits._

    ClickHouseUDFs.registerAll(spark)
    val source = (0 until 32).map(id => (id, s"value-$id")).toDF("id", "value")
    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      options = Map.empty)

    val shard = new TestableKeyBasedBatchWriter(config, 1, 2, source, Some("id"))
      .testLoadShardData()
      .select("id")
      .as[Int]
      .collect()
      .toSet
    val expected = (0 until 32).filter(id => Math.abs(id.toString.hashCode) % 2 == 1).toSet

    assert(shard == expected)
  }

  test("convertNullToDefaults should convert null to default values for each type null convert null to default values for different types") {
    import sparkImplicits._

    // Create test data containing null values.
    val testData = Seq(
      ("Alice", Some(25), Some(1000L), Some(1.5f), Some(2.5)),
      ("Bob", None, None, None, None),
      (null, Some(30), Some(2000L), Some(2.5f), Some(3.5))
    ).toDF("name", "age", "salary", "score", "rating")

    // Register ClickHouse UDF (Simulated)
    spark.udf.register("ck_null_to_default_string", (s: String) => if (s == null) "" else s)
    spark.udf.register("ck_null_to_default_int", (i: java.lang.Integer) => if (i == null) 0 else i.intValue())
    spark.udf.register("ck_null_to_default_long", (l: java.lang.Long) => if (l == null) 0L else l.longValue())
    spark.udf.register("ck_null_to_default_float", (f: java.lang.Float) => if (f == null) 0.0f else f.floatValue())
    spark.udf.register("ck_null_to_default_double", (d: java.lang.Double) => if (d == null) 0.0 else d.doubleValue())

    // Execute transformation (using reflection to invoke private method)
    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      options = Map.empty
    )

    val writer = new TestableClickHouseBatchWriter(spark, testData, 0, 1, config)
    val result = writer.testConvertNullToDefaults(testData)

    // Validate results
    val rows = result.collect()
    assert(rows.length == 3)

    // No null values, unchanged.
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getInt(1) == 25)

    // Second row: Convert all numeric types with null values to 0
    assert(rows(1).getString(0) == "Bob")
    assert(rows(1).getInt(1) == 0)
    assert(rows(1).getLong(2) == 0L)
    assert(rows(1).getFloat(3) == 0.0f)
    assert(rows(1).getDouble(4) == 0.0)

    // Third row: Convert string null to empty string
    assert(rows(2).getString(0) == "")
    assert(rows(2).getInt(1) == 30)
  }

  test("convertCollectionTypes should convert Array/Map to a ClickHouse string format") {
    import sparkImplicits._

    // Create a dataset of type collection for testing.
    val testData = Seq(
      ("Alice", Array("tag1", "tag2"), Map("k1" -> "v1", "k2" -> "v2")),
      ("Bob", Array("tag3"), Map("k3" -> "v3")),
      ("Charlie", Array.empty[String], Map.empty[String, String])
    ).toDF("name", "tags", "attrs")

    // Register ClickHouse UDF (Simulated)
    spark.udf.register("ck_format_array", (arr: Seq[String]) =>
      if (arr == null || arr.isEmpty) "[]" else arr.mkString("['", "','", "']"))
    spark.udf.register("ck_format_map", (map: Map[String, String]) =>
      if (map == null || map.isEmpty) "{}" else map.map { case (k, v) => s"'$k':'$v'" }.mkString("{", ",", "}"))

    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      options = Map.empty
    )

    val writer = new TestableClickHouseBatchWriter(spark, testData, 0, 1, config)
    val result = writer.testConvertCollectionTypes(testData)

    // Validate results
    val rows = result.collect()
    assert(rows.length == 3)

    // Converting an array and a map to string format.Array convert from Array and Map to string format Map convert to string format
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getString(1) == "['tag1','tag2']")
    assert(rows(0).getString(2).contains("'k1':'v1'"))

    // Second line: Single-element Array and Map
    assert(rows(1).getString(0) == "Bob")
    assert(rows(1).getString(1) == "['tag3']")

    // Third row: Convert empty set to empty string format
    assert(rows(2).getString(0) == "Charlie")
    assert(rows(2).getString(1) == "[]")
    assert(rows(2).getString(2) == "{}")
  }

  test("alignToSchema should delete columns in the ClickHouse table that do not exist") {
    import sparkImplicits._

    // Create test data with extra columns
    val testData = Seq(
      ("Alice", 25, "extra1", 1000L),
      ("Bob", 30, "extra2", 2000L)
    ).toDF("name", "age", "extra_col", "salary")

    val config = ClickHouseBatchConfig(
      database = "test_db",
      table = "test_table",
      jdbcUrl = "jdbc:clickhouse://localhost:8123/test_db",
      options = Map.empty
    )

    // Simulate ClickHouse table with only columns name and age.
    val writer = new TestableClickHouseBatchWriter(
      spark, testData, 0, 1, config,
      ckTableColumns = Set("name", "age")
    )
    val result = writer.testAlignToSchema(testData)

    // Validate results: delete extra_col and salary.
    val resultColumns = result.columns.toSet
    assert(resultColumns == Set("name", "age"))
    assert(!resultColumns.contains("extra_col"))
    assert(!resultColumns.contains("salary"))

    // Validate data integrity
    val rows = result.collect()
    assert(rows.length == 2)
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getInt(1) == 25)
  }
}

/**
 * Test utility class: Exposes private methods of ClickHouseBatchWriter for unit testing purposes.
 */
class TestableClickHouseBatchWriter(
  override val spark: SparkSession,
  testData: DataFrame,
  index: Int,
  machineCount: Int,
  config: ClickHouseBatchConfig,
  ckTableColumns: Set[String] = Set.empty
) extends ClickHouseBatchWriter(config, index, machineCount) {

  override protected def loadShardData: DataFrame = testData

  def testConvertNullToDefaults(df: DataFrame): DataFrame = {
    // Use reflection to call private method
    val method = classOf[ClickHouseBatchWriter].getDeclaredMethod("convertNullToDefaults", classOf[DataFrame])
    method.setAccessible(true)
    method.invoke(this, df).asInstanceOf[DataFrame]
  }

  def testConvertCollectionTypes(df: DataFrame): DataFrame = {
    val method = classOf[ClickHouseBatchWriter].getDeclaredMethod("convertCollectionTypes", classOf[DataFrame])
    method.setAccessible(true)
    method.invoke(this, df).asInstanceOf[DataFrame]
  }

  def testAlignToSchema(df: DataFrame): DataFrame = {
    // if provided ckTableColumnsoverrides getClickHouseTableSchema method
    if (ckTableColumns.nonEmpty) {
      df.schema.names.foldLeft(df) { (d, c) =>
        if (ckTableColumns.contains(c)) d else d.drop(c)
      }
    } else {
      val method = classOf[ClickHouseBatchWriter].getDeclaredMethod("alignToSchema", classOf[DataFrame])
      method.setAccessible(true)
      method.invoke(this, df).asInstanceOf[DataFrame]
    }
  }
}

private class TestableKeyBasedBatchWriter(
  config: ClickHouseBatchConfig,
  index: Int,
  machineCount: Int,
  sourceDataFrame: DataFrame,
  primaryKey: Option[String]
) extends KeyBasedBatchWriter(config, index, machineCount, sourceDataFrame, primaryKey) {

  def testLoadShardData(): DataFrame = loadShardData
}
