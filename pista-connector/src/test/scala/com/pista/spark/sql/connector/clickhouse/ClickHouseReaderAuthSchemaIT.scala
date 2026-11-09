package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.reader.{InputConfig, ReaderRegistry}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}

import java.util.{Properties, UUID}

class ClickHouseReaderAuthSchemaIT extends ClickHouseITSupport {
  test("default ClickHouse Reader uses configured credentials and Spark JDBC schema") {
    val (database, table) = prepareTable(
      "id Int32, value String, note Nullable(String), amount Decimal(10, 2)", "it_ck_reader_auth")
    ckContainer.executeOnEntry(
      s"INSERT INTO $database.$table VALUES (1, 'one', NULL, 12.34), (2, 'two', 'note', 56.78)")
    val endpoint = clickHouseNodes.head.split(":")
    val options = Map("clickhouse.host" -> endpoint(0), "clickhouse.port" -> endpoint(1),
      "clickhouse.username" -> ckContainer.username, "clickhouse.password" -> ckContainer.userPassword)
    val config = InputConfig(enabled = true, path = Some(s"$database.$table"),
      format = "clickhouse", options = options)
    val reader = new ClickHouseReader
    val expected = spark.read.format("jdbc")
      .option("url", s"jdbc:clickhouse://${endpoint(0)}:${endpoint(1)}/$database")
      .option("dbtable", table).option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("user", ckContainer.username).option("password", ckContainer.userPassword).load()
    val expectedSchema = expected.schema
    assert(expectedSchema("id").nullable && expectedSchema("note").nullable)
    assert(expectedSchema("amount").dataType == DecimalType(10, 2))
    assert(reader.inferSchema(spark, config).contains(expectedSchema))

    def checkRead(input: InputConfig): Unit = {
      val df = ReaderRegistry.read(spark, input)
      assert(df.queryExecution.optimizedPlan.toString.contains("JDBCRelation"))
      assert(df.schema == expectedSchema)
      val rows = df.collect().map { row =>
        (row.getInt(0), row.getString(1), Option(row.getString(2)), row.getDecimal(3).toPlainString)
      }.toSet
      assert(rows == Set((1, "one", None, "12.34"), (2, "two", Some("note"), "56.78")))
    }

    checkRead(config.copy(options = options + ("force.jdbc" -> "true")))
    checkRead(config)
    checkRead(config.copy(schema = Some(expectedSchema), options = options + ("force.jdbc" -> "true")))
    Seq(true, false).foreach { force =>
      val jdbcOptions = if (force) options + ("force.jdbc" -> "true") else options
      val original = config.copy(options = jdbcOptions, predicates = Seq("id = 1"), columns = Seq("id"))
      val originalResult = ReaderRegistry.read(spark, original)
      assert(originalResult.columns.toSeq == Seq("id"))
      assert(originalResult.collect().map(_.getInt(0)).toSeq == Seq(1))
      val selected = config.copy(options = jdbcOptions,
        predicates = Seq("value = 'one'", "id > 0"), columns = Seq("id"))
      val filtered = ReaderRegistry.read(spark, selected)
      assert(filtered.queryExecution.optimizedPlan.toString.contains("JDBCRelation"))
      assert(filtered.columns.toSeq == Seq("id"))
      assert(filtered.collect().map(_.getInt(0)).toSeq == Seq(1))
      val disjoint = selected.copy(predicates = Seq("value = 'one'", "id = 2"))
      assert(ReaderRegistry.read(spark, disjoint).collect().isEmpty)
    }
    val incompatible = StructType(expectedSchema.fields.map { field =>
      if (field.name == "id") field.copy(dataType = StringType) else field
    })
    intercept[Exception] {
      ReaderRegistry.read(spark, config.copy(schema = Some(incompatible),
        options = options + ("force.jdbc" -> "true"))).collect()
    }

    val invalidCredential = "pista-invalid-credential-" + UUID.randomUUID().toString
    val error = intercept[Exception] {
      ReaderRegistry.read(spark, config.copy(options = options ++ Map(
        "force.jdbc" -> "true", "clickhouse.password" -> invalidCredential))).collect()
    }
    def hasNoCredential(cause: Throwable): Boolean =
      !cause.toString.contains(invalidCredential) &&
        Option(cause.getCause).forall(hasNoCredential) && cause.getSuppressed.forall(hasNoCredential)
    assert(hasNoCredential(error))

    var connectorSchema: Option[StructType] = None
    val connectorSpy = new ClickHouseReader {
      override protected def estimateRowCount(url: String, table: String, props: Properties): Option[Long] =
        Some(smallDataThreshold)
      override protected def readViaConnector(
        spark: SparkSession, schema: Option[StructType], config: InputConfig
      ): DataFrame = {
        connectorSchema = schema
        spark.emptyDataFrame
      }
    }
    connectorSpy.read(spark, config)
    assert(connectorSchema.contains(expectedSchema))
    connectorSchema = None
    connectorSpy.read(spark, config.copy(schema = Some(expectedSchema)))
    assert(connectorSchema.contains(expectedSchema))
  }

  test("ClickHouse JDBC preserves duplicate rows and evaluates composed filters") {
    val definition = "id Int32, payload Nullable(Int32), `odd.name` Int32"
    val (database, table) = prepareTable(definition, "it_ck_reader_correctness")
    ckContainer.executeOnEntry(
      s"INSERT INTO $database.$table VALUES (1, 11, 101), (1, 11, 101), (2, NULL, 202), (3, 33, 303)")
    val endpoint = clickHouseNodes.head.split(":")
    val options = Map("clickhouse.host" -> endpoint(0), "clickhouse.port" -> endpoint(1),
      "clickhouse.username" -> ckContainer.username, "clickhouse.password" -> ckContainer.userPassword)
    val config = InputConfig(enabled = true, path = Some(s"$database.$table"),
      format = "clickhouse", options = options)
    val oracle = withClickHouseConnection { connection =>
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(s"SELECT id, payload, `odd.name` FROM $database.$table")
        try {
          val rows = scala.collection.mutable.ArrayBuffer.empty[(Int, Option[Int], Int)]
          while (result.next()) {
            val id = result.getInt(1)
            val payload = result.getInt(2)
            val optionalPayload = if (result.wasNull()) None else Some(payload)
            rows += ((id, optionalPayload, result.getInt(3)))
          }
          rows.toSeq.sortBy(row => (row._1, row._3))
        } finally result.close()
      } finally statement.close()
    }
    assert(oracle == Seq((1, Some(11), 101), (1, Some(11), 101),
      (2, None, 202), (3, Some(33), 303)))

    def read(input: InputConfig): DataFrame = ReaderRegistry.read(spark, input)
    def projected(df: DataFrame): Seq[(Int, Int)] =
      df.collect().map(row => (row.getInt(0), row.getInt(1))).toSeq.sortBy(identity)

    Seq(true, false).foreach { force =>
      val jdbcOptions = if (force) options + ("force.jdbc" -> "true") else options
      val base = config.copy(options = jdbcOptions)
      val all = read(base)
      assert(all.queryExecution.optimizedPlan.toString.contains("JDBCRelation"))
      assert(all.columns.toSeq == Seq("id", "payload", "odd.name"))
      val rows = all.collect().map { row =>
        (row.getInt(0), if (row.isNullAt(1)) None else Some(row.getInt(1)), row.getInt(2))
      }.toSeq.sortBy(row => (row._1, row._3))
      assert(rows == oracle)
      val grouped = base.copy(predicates = Seq("id = 1 OR id = 2", "id = 2"),
        columns = Seq("odd.name", "id"))
      val groupedResult = read(grouped)
      assert(groupedResult.columns.toSeq == Seq("odd.name", "id"))
      assert(projected(groupedResult) == Seq((202, 2)))
      val nullRows = base.copy(predicates = Seq("payload IS NULL"), columns = Seq("id", "odd.name"))
      assert(projected(read(nullRows)) == Seq((2, 202)))
      val nonNullRows = base.copy(predicates = Seq("payload IS NOT NULL"), columns = Seq("id", "odd.name"))
      assert(projected(read(nonNullRows)) == Seq((1, 101), (1, 101), (3, 303)))
      val nullEquality = base.copy(predicates = Seq("payload = NULL"), columns = Seq("id"))
      assert(read(nullEquality).collect().isEmpty)
    }

    val noPushdown = new ClickHouseReader {
      override protected def buildJdbcProperties(options: Map[String, String]): Properties = {
        val props = super.buildJdbcProperties(options)
        props.setProperty("pushDownPredicate", "false")
        props
      }
    }
    val residual = noPushdown.read(spark, config.copy(options = options + ("force.jdbc" -> "true"),
      predicates = Seq("id = 1 OR id = 2", "id = 2"), columns = Seq("odd.name", "id")))
    assert(residual.queryExecution.executedPlan.collect { case _: FilterExec => 1 }.nonEmpty)
    assert(projected(residual) == Seq((202, 2)))

    val (emptyDatabase, emptyTable) = prepareTable(definition, "it_ck_reader_empty")
    val empty = read(config.copy(path = Some(s"$emptyDatabase.$emptyTable")))
    assert(empty.columns.toSeq == Seq("id", "payload", "odd.name"))
    assert(empty.collect().isEmpty)
  }
}
