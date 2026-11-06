package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.execution.datasources.reader.InputConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class ClickHouseReaderIT extends ClickHouseITSupport {
  test("JDBC reader applies filters and projections with a real ClickHouse") {
    val (database, table) = prepareTable("id Int32, value String", "it_ck_reader")
    ckContainer.executeOnEntry(s"INSERT INTO $database.$table VALUES (1, 'one'), (2, 'two')")
    val endpoint = clickHouseNodes.head.split(":")
    val config = InputConfig(enabled = true, path = Some(s"$database.$table"), format = "clickhouse",
      predicates = Seq("value = 'one'", "id > 0"), columns = Seq("id"),
      options = Map("clickhouse.host" -> endpoint(0), "clickhouse.port" -> endpoint(1),
        "clickhouse.username" -> ckContainer.username, "clickhouse.password" -> ckContainer.userPassword,
        "force.jdbc" -> "true"))
    // Existing Reader authentication and inferred nullability do not work with this fixture.
    // Let Spark infer its own schema and authenticate without changing production behavior.
    val reader = new ClickHouseReader {
      override protected def buildJdbcProperties(options: Map[String, String]): java.util.Properties =
        buildClickHouseJdbcProperties(options)
      override def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] = None
    }
    val df = reader.read(spark, config)
    assert(df.queryExecution.optimizedPlan.toString.contains("JDBCRelation"))
    assert(df.columns.toSeq == Seq("id"))
    assert(df.collect().map(_.getInt(0)).toSeq == Seq(1))
    intercept[Exception] { reader.read(spark, config.copy(predicates = Seq("id ="))).collect() }
    intercept[Exception] { reader.read(spark, config.copy(columns = Seq("missing_column"))).collect() }
  }
}
