package com.pista.spark.sql.metrics.collector

import org.apache.spark.sql.{Observation, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Data quality extractor test {}
 *
 * Covers the complete buildObserveExprs and parseFromObservation paths,
 * Column count, per-column null statistics, minimum, maximum, average, and standard deviation for numeric columns.
 *
 */
class DataQualityExtractorSuite extends AnyFunSuite {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("DataQualityExtractorSuite")
    .getOrCreate()

  private val extractor = new DataQualityExtractor()

  /** Trigger the observation of an Action and parse the results of a helper method */
  private def observeAndParse(
    df: org.apache.spark.sql.DataFrame,
    obsName: String
  ) = {
    val (exprs, meta) = extractor.buildObserveExprs(df, "test-app", 1L, "test_table")
    val obs           = Observation(obsName)
    val observed      = df.observe(obs, exprs.head, exprs.tail: _*)
    observed.count()
    extractor.parseFromObservation(obs.get, meta)
  }

  test("No null values data totalRecords correct") {
    import spark.implicits._
    val df      = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val metrics = observeAndParse(df, "obs_no_nulls")

    assert(metrics.totalRecords == 3)
    assert(metrics.metricType == "data_quality")
    assert(metrics.columnStats("id").nullCount == 0)
    assert(metrics.columnStats("name").nullCount == 0)
  }

  test("column nullCount and nullRatio reflect null values") {
    import spark.implicits._
    val df = Seq(
      (Some(1), Some("Alice")),
      (Some(2), None),
      (None,    Some("Charlie"))
    ).toDF("id", "name")
    val metrics = observeAndParse(df, "obs_with_nulls")

    assert(metrics.totalRecords == 3)
    // id column: 1 null value
    assert(metrics.columnStats("id").nullCount == 1)
    assert(Math.abs(metrics.columnStats("id").nullRatio - (1.0 / 3)) < 1e-9)
    // name column: 1 null value
    assert(metrics.columnStats("name").nullCount == 1)
    assert(Math.abs(metrics.columnStats("name").nullRatio - (1.0 / 3)) < 1e-9)
  }

  test("numeric columnStats include min, max, and average metrics") {
    import spark.implicits._
    val df      = Seq((10, "a"), (20, "b"), (30, "c")).toDF("value", "name")
    val metrics = observeAndParse(df, "obs_numeric")

    val valueStats = metrics.columnStats("value")
    assert(valueStats.minValue.contains(10))   // IntegerType Preserve original type
    assert(valueStats.maxValue.contains(30))
    assert(valueStats.avgValue.contains(20.0))
    assert(valueStats.stdDevValue.isDefined)
  }

  test("string columnStats omit numeric statistics") {
    import spark.implicits._
    val df      = Seq("Alice", "Bob", "Charlie").toDF("name")
    val metrics = observeAndParse(df, "obs_string_only")

    val nameStats = metrics.columnStats("name")
    assert(nameStats.minValue.isEmpty)
    assert(nameStats.maxValue.isEmpty)
    assert(nameStats.avgValue.isEmpty)
    assert(nameStats.stdDevValue.isEmpty)
  }

  test("independent statistics for each numerical column") {
    import spark.implicits._
    val df = Seq(
      (1, 100.0, "a"),
      (2, 200.0, "b"),
      (3, 300.0, "c")
    ).toDF("id", "price", "name")
    val metrics = observeAndParse(df, "obs_multi_numeric")

    assert(metrics.columnStats("id").minValue.contains(1))     // IntegerType
    assert(metrics.columnStats("id").maxValue.contains(3))
    assert(metrics.columnStats("price").minValue.contains(100.0))  // DoubleType
    assert(metrics.columnStats("price").maxValue.contains(300.0))
    assert(metrics.columnStats("name").minValue.isEmpty)
  }

  test("empty DataFrame safe handling") {
    val df      = spark.emptyDataFrame
    val metrics = observeAndParse(df, "obs_empty")

    assert(metrics.totalRecords == 0)
    assert(metrics.columnStats.isEmpty)
  }

  test("per-column nullCount correct output columnStats") {
    import spark.implicits._
    val df = Seq(
      (Some(1), Some("Alice")),
      (Some(2), None),
      (None,    Some("Charlie"))
    ).toDF("id", "name")
    val metrics = observeAndParse(df, "obs_col_stats")

    assert(metrics.columnStats.contains("id"))
    assert(metrics.columnStats.contains("name"))
    assert(metrics.columnStats("id").nullCount == 1)
    assert(metrics.columnStats("name").nullCount == 1)
  }

  test("approxDistinctCount Correct Output") {
    import spark.implicits._
    // 3 rows with distinct values:approxDistinctCount converge to 3
    val df      = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    val metrics = observeAndParse(df, "obs_hll")

    val idStats = metrics.columnStats("id")
    assert(idStats.approxDistinctCount.isDefined)
    assert(idStats.approxDistinctCount.get > 0)

    val nameStats = metrics.columnStats("name")
    assert(nameStats.approxDistinctCount.isDefined)
    assert(nameStats.approxDistinctCount.get > 0)
  }

  test("numeric min, max, and average ignore null values") {
    import spark.implicits._
    val df = Seq(
      (Some(10), "a"),
      (None,     "b"),
      (Some(30), "c")
    ).toDF("value", "name")
    val metrics = observeAndParse(df, "obs_numeric_with_nulls")

    val valueStats = metrics.columnStats("value")
    assert(valueStats.nullCount == 1)
    assert(valueStats.minValue.contains(10))   // IntegerType Preserve original type
    assert(valueStats.maxValue.contains(30))
    assert(valueStats.avgValue.contains(20.0))
  }

  test("column outputs empty/blank/lengthP50/lengthP95") {
    import spark.implicits._
    val df = Seq(
      Tuple1(Some("")),
      Tuple1(Some("   ")),
      Tuple1(Some("ab")),
      Tuple1(Some("a")),
      Tuple1(None)
    ).toDF("name")
    val metrics = observeAndParse(df, "obs_string_quality")

    val nameStats = metrics.columnStats("name")
    assert(nameStats.emptyStringCount.contains(1L))
    assert(nameStats.blankStringCount.contains(2L))
    assert(nameStats.lengthP50.isDefined)
    assert(nameStats.lengthP95.isDefined)
    assert(nameStats.lengthP50.get >= 0L && nameStats.lengthP50.get <= 3L)
    assert(nameStats.lengthP95.get >= nameStats.lengthP50.get)
  }

  test("the value column outputs p50/p95/p99") {
    import spark.implicits._
    val df = (1 to 100).map(i => (i, s"name_$i")).toDF("value", "name")
    val metrics = observeAndParse(df, "obs_numeric_percentiles")

    val valueStats = metrics.columnStats("value")
    assert(valueStats.p50Value.isDefined)
    assert(valueStats.p95Value.isDefined)
    assert(valueStats.p99Value.isDefined)

    // value column as Int type, ensure the quantile field type remains as Int.
    assert(valueStats.p50Value.get.isInstanceOf[Int])
    assert(valueStats.p95Value.get.isInstanceOf[Int])
    assert(valueStats.p99Value.get.isInstanceOf[Int])

    val p50 = valueStats.p50Value.get.asInstanceOf[Int]
    val p95 = valueStats.p95Value.get.asInstanceOf[Int]
    val p99 = valueStats.p99Value.get.asInstanceOf[Int]
    assert(p50 <= p95)
    assert(p95 <= p99)
    assert(p50 >= 40 && p50 <= 60)
    assert(p95 >= 85 && p95 <= 100)
    assert(p99 >= 90 && p99 <= 100)
  }
}
