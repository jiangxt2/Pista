package com.pista.spark.sql.functions

import com.pista.spark.sql.catalyst.functions.catalog.{PistaFunctionCatalog, TableFunctionDefinition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PistaFunctionSqlSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("PistaFunctionSqlSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    PistaFunctionInstaller.install(spark)
  }

  override protected def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("explicit installation is complete and idempotent") {
    val report = PistaFunctionInstaller.install(spark)
    assert(report.catalogDigest == PistaFunctionCatalog.digest)
    assert(report.installed.isEmpty)
    assert(report.alreadyPresent.size == 30)
    PistaFunctionInstaller.validate(spark)
  }

  test("all scalar families resolve through SQL") {
    val row = spark.sql(
      """SELECT
        |  hex(pista_ip_to_binary('192.168.1.1')),
        |  pista_try_ip_to_binary('bad') IS NULL,
        |  pista_binary_to_ip(unhex('C0A80101')),
        |  pista_ipv4_to_long('192.168.1.1'),
        |  pista_long_to_ipv4(3232235777L),
        |  pista_ip_family('2001:db8::1'),
        |  pista_ip_is_private('fd00::1'),
        |  pista_ip_in_cidr('10.1.2.3', '10.0.0.0/8'),
        |  hex(pista_ip_prefix('10.1.2.3', 16)),
        |  pista_try_parse_date('2024-02-29', array('uuuu-MM-dd')),
        |  pista_try_parse_timestamp('2024-02-29 12:30:00', array('uuuu-MM-dd HH:mm:ss'), 'UTC'),
        |  pista_json_is_valid('{"a":1}'),
        |  pista_json_merge_patch('{"a":1}', '{"b":2}'),
        |  pista_registrable_domain('www.example.co.uk'),
        |  pista_try_url_decode('a+b%2Fc'),
        |  pista_vector_inner_product(array(1D, 2D), array(3D, 4D)),
        |  pista_vector_l2_distance(array(0D, 0D), array(3D, 4D)),
        |  pista_vector_cosine_similarity(array(1D, 0D), array(1D, 0D)),
        |  pista_vector_l2_normalize(array(3D, 4D))
        |""".stripMargin).head()
    assert(row.getString(0) == "C0A80101")
    assert(row.getBoolean(1))
    assert(row.getString(2) == "192.168.1.1")
    assert(row.getLong(3) == 3232235777L)
    assert(row.getString(4) == "192.168.1.1")
    assert(row.getInt(5) == 6)
    assert(row.getBoolean(6) && row.getBoolean(7))
    assert(row.getString(8) == "0A010000")
    assert(row.getDate(9).toString == "2024-02-29")
    assert(!row.isNullAt(10))
    assert(row.getBoolean(11))
    assert(row.getString(12) == "{\"a\":1,\"b\":2}")
    assert(row.getString(13) == "example.co.uk")
    assert(row.getString(14) == "a b/c")
    assert(row.getDouble(15) == 11.0)
    assert(row.getDouble(16) == 5.0)
    assert(row.getDouble(17) == 1.0)
    assert(row.getSeq[Double](18) == Seq(0.6, 0.8))
  }

  test("Roaring64 scalar, aggregate, and table functions compose") {
    val aggregate = spark.sql(
      """SELECT pista_roaring64_build(id) AS built
        |FROM VALUES (3L), (1L), (3L), (NULL) AS t(id)""".stripMargin).head().getAs[Array[Byte]](0)
    val cardinality = spark.createDataFrame(Seq(Tuple1(aggregate))).toDF("bitmap")
      .selectExpr("pista_roaring64_cardinality(bitmap)").head().getLong(0)
    assert(cardinality == 2L)

    spark.createDataFrame(Seq(Tuple1(aggregate), Tuple1(aggregate))).toDF("bitmap")
      .createOrReplaceTempView("cohorts")
    assert(spark.sql(
      "SELECT pista_roaring64_cardinality(pista_roaring64_union_agg(bitmap)) FROM cohorts")
      .head().getLong(0) == 2L)

    val operations = spark.sql(
      """WITH bitmaps AS (
        |  SELECT pista_roaring64_from_array(array(1L, 3L)) AS left,
        |         pista_roaring64_from_array(array(3L, 5L)) AS right
        |)
        |SELECT
        |  pista_roaring64_cardinality(pista_roaring64_union(left, right)),
        |  pista_roaring64_cardinality(pista_roaring64_intersect(left, right)),
        |  pista_roaring64_cardinality(pista_roaring64_xor(left, right)),
        |  pista_roaring64_cardinality(pista_roaring64_and_not(left, right)),
        |  pista_roaring64_contains(left, 3L)
        |FROM bitmaps""".stripMargin).head()
    assert(operations.getLong(0) == 3L)
    assert(operations.getLong(1) == 1L)
    assert(operations.getLong(2) == 2L)
    assert(operations.getLong(3) == 1L)
    assert(operations.getBoolean(4))

    val rows = spark.sql(
      "SELECT value FROM pista_roaring64_explode(pista_roaring64_from_array(array(3L, 1L)), 10)")
      .collect().map(_.getLong(0)).toSeq
    assert(rows == Seq(1L, 3L))
  }

  test("vector centroid performs partial merge and ignores null rows") {
    val frame = spark.sql(
      """SELECT pista_vector_centroid(value)
        |FROM VALUES (array(1D, 2D)), (array(3D, 4D)), (CAST(NULL AS ARRAY<DOUBLE>)) AS t(value)
        |""".stripMargin)
    val row = frame.head()
    assert(row.getSeq[Double](0) == Seq(2.0, 3.0))
    assert("pista_vector_centroid".r.findAllIn(frame.queryExecution.executedPlan.toString()).length >= 2)

    val localSpark = spark
    import localSpark.implicits._
    val cancellation = Seq(1.0e16, 1.0, -1.0e16).toDF("value").repartition(3)
      .selectExpr("array(value) AS value")
      .agg(org.apache.spark.sql.functions.call_function("pista_vector_centroid",
        org.apache.spark.sql.functions.col("value")).as("centroid"))
      .head().getSeq[Double](0).head
    assert(Math.abs(cancellation - (1.0 / 3.0)) < 1e-12)
  }

  test("scalar execution remains code-generated for non-foldable rows") {
    val frame = spark.range(1, 5).selectExpr(
      "pista_long_to_ipv4(id) AS ip",
      "pista_vector_inner_product(array(CAST(id AS DOUBLE), 2D), array(3D, 4D)) AS score",
      "pista_roaring64_cardinality(pista_roaring64_from_array(array(id))) AS cohort_size")
    assert(frame.collect().length == 4)
    assert(frame.queryExecution.executedPlan.collect { case node: WholeStageCodegenExec => node }.nonEmpty)
  }

  test("interpreted and code-generated execution are equivalent") {
    val codegenKey = SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key
    val factoryModeKey = SQLConf.CODEGEN_FACTORY_MODE.key
    val previous = spark.conf.get(codegenKey)
    val previousFactoryMode = spark.conf.get(factoryModeKey)
    val query = () => {
      val frame = spark.range(1, 5).selectExpr(
        "pista_long_to_ipv4(id) AS ip",
        "pista_vector_inner_product(array(CAST(id AS DOUBLE), 2D), array(3D, 4D)) AS score",
        "pista_roaring64_cardinality(pista_roaring64_from_array(array(id))) AS cohort_size")
      val rows = frame.collect().toSeq
      val hasWholeStage = frame.queryExecution.executedPlan
        .collect { case node: WholeStageCodegenExec => node }.nonEmpty
      (rows, hasWholeStage)
    }
    try {
      spark.conf.set(codegenKey, "true")
      val codeGenerated = query()
      spark.conf.set(codegenKey, "false")
      spark.conf.set(factoryModeKey, "NO_CODEGEN")
      val interpreted = query()
      assert(codeGenerated._2)
      assert(!interpreted._2)
      assert(interpreted._1 == codeGenerated._1)
    } finally {
      spark.conf.set(codegenKey, previous)
      spark.conf.set(factoryModeKey, previousFactoryMode)
    }
  }

  test("try date and timestamp functions match Spark builtins on canonical inputs") {
    val timezoneKey = SQLConf.SESSION_LOCAL_TIMEZONE.key
    val previous = spark.conf.get(timezoneKey)
    try {
      spark.conf.set(timezoneKey, "UTC")
      val rows = spark.sql(
        """SELECT date_value,
          |  pista_try_parse_date(date_value, array('uuuu-MM-dd')) AS pista_date,
          |  to_date(date_value, 'yyyy-MM-dd') AS spark_date,
          |  pista_try_parse_timestamp(timestamp_value, array('uuuu-MM-dd HH:mm:ss'), 'UTC') AS pista_ts,
          |  to_timestamp(timestamp_value, 'yyyy-MM-dd HH:mm:ss') AS spark_ts
          |FROM VALUES ('2024-02-29', '2024-02-29 12:30:00'), ('not-a-date', 'not-a-timestamp')
          |AS input(date_value, timestamp_value)""".stripMargin).collect()
      rows.foreach { row =>
        assert(row.isNullAt(1) == row.isNullAt(2))
        assert(row.isNullAt(3) == row.isNullAt(4))
        if (!row.isNullAt(1)) assert(row.get(1) == row.get(2))
        if (!row.isNullAt(3)) assert(row.get(3) == row.get(4))
      }
    } finally {
      spark.conf.set(timezoneKey, previous)
    }
  }

  test("extension installation exposes the same catalog") {
    spark.stop()
    spark = SparkSession.builder()
      .appName("PistaFunctionExtensionSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", classOf[PistaSparkSessionExtensions].getName)
      .getOrCreate()
    assert(spark.sql("SELECT pista_ip_family('127.0.0.1')").head().getInt(0) == 4)
    PistaFunctionInstaller.validate(spark)
  }

  test("invalid inputs and non-foldable control arguments fail closed") {
    assertThrows[Exception](spark.sql("SELECT pista_ip_to_binary('example.com')").collect())
    assertThrows[Exception](spark.sql("SELECT pista_binary_to_ip(unhex('0102'))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT pista_roaring64_from_array(array(1L, -1L))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT pista_vector_inner_product(array(1D), array(1D, 2D))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT pista_vector_l2_normalize(array(0D, 0D))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT pista_try_parse_date(value, formats) " +
        "FROM VALUES ('20240101', array('uuuuMMdd')) AS input(value, formats)").collect())
    assertThrows[Exception](spark.sql(
      "SELECT value FROM pista_roaring64_explode(" +
        "pista_roaring64_from_array(array(1L, 2L)), 1)").collect())
    assertThrows[Exception](spark.sql(
      "SELECT value FROM pista_roaring64_explode(" +
        "pista_roaring64_from_array(array(1L, 2L)), CAST(NULL AS INT))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT value FROM pista_roaring64_explode(" +
        "pista_roaring64_from_array(array(1L, 2L)), 0)").collect())
    assertThrows[Exception](spark.sql(
      "SELECT value FROM pista_roaring64_explode(" +
        "pista_roaring64_from_array(array(1L, 2L)), CAST(10 AS BIGINT))").collect())
    assertThrows[Exception](spark.sql(
      "SELECT value FROM pista_roaring64_explode(" +
        "pista_roaring64_from_array(array(1L, 2L)), 1000001)").collect())
  }

  test("representative nulls propagate without invoking strict kernels") {
    val row = spark.sql(
      """SELECT
        |  pista_ip_to_binary(CAST(NULL AS STRING)),
        |  pista_try_parse_date(CAST(NULL AS STRING), array('uuuu-MM-dd')),
        |  pista_json_merge_patch(CAST(NULL AS STRING), '{}'),
        |  pista_roaring64_cardinality(CAST(NULL AS BINARY)),
        |  pista_vector_l2_normalize(CAST(NULL AS ARRAY<DOUBLE>))
        |""".stripMargin).head()
    assert((0 until row.size).forall(row.isNullAt))
  }

  test("explicit installation rejects an existing non-Pista owner before partial registration") {
    spark.stop()
    spark = SparkSession.builder()
      .appName("PistaFunctionConflictSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.udf.register("pista_ip_family", (_: String) => 0)

    val error = intercept[Exception](PistaFunctionInstaller.install(spark))
    assert(error.getMessage.contains("pista_ip_family"))
    assert(spark.sessionState.functionRegistry
      .lookupFunction(FunctionIdentifier("pista_json_is_valid")).isEmpty)
    assert(spark.sessionState.tableFunctionRegistry
      .lookupFunction(FunctionIdentifier("pista_roaring64_explode")).isEmpty)
  }

  test("explicit installation rejects a Pista name in the other registry namespace") {
    spark.stop()
    spark = SparkSession.builder()
      .appName("PistaFunctionCrossRegistryConflictSuite")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val tableDefinition = PistaFunctionCatalog.definitions.collectFirst {
      case definition: TableFunctionDefinition => definition
    }.get
    spark.sessionState.tableFunctionRegistry.registerFunction(
      FunctionIdentifier("pista_ip_family"),
      tableDefinition.expressionInfo,
      tableDefinition.builder)

    val error = intercept[Exception](PistaFunctionInstaller.install(spark))
    assert(error.getMessage.contains("pista_ip_family"))
    assert(error.getMessage.contains("other-registry"))
    assert(spark.sessionState.functionRegistry
      .lookupFunction(FunctionIdentifier("pista_json_is_valid")).isEmpty)
  }
}
