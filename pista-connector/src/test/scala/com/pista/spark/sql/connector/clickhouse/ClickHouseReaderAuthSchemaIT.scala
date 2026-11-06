package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.reader.{InputConfig, ReaderRegistry}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
      .option("http_connection_provider", "HTTP_URL_CONNECTION")
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
}
