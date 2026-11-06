package com.pista.spark.sql.connector.doris

import com.pista.spark.sql.execution.datasources.reader.{InputConfig, ReaderRegistry}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FilterExec

import java.util.Properties

class DorisReaderIT extends DorisITSupport {
  test("JDBC reader applies predicates before the requested projection") {
    val (database, table) = createDorisTable("it_doris_reader",
      """(id INT, value VARCHAR(128)) DUPLICATE KEY(id)
        |DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ("replication_num" = "2")""".stripMargin)
    executeDoris(s"INSERT INTO `$database`.`$table` VALUES (1, 'one'), (2, 'two')")
    val config = InputConfig(enabled = true, path = Some(s"$database.$table"), format = "doris",
      options = Map("doris.fenodes" -> fenodes, "doris.query.port" -> feQueryPort.toString,
        "doris.user" -> "root", "doris.password" -> "", "force.jdbc" -> "true"))

    def read(predicates: Seq[String] = Nil, columns: Seq[String] = Nil,
             force: Boolean = true): DataFrame = {
      val options = if (force) config.options else config.options - "force.jdbc"
      val df = ReaderRegistry.read(spark, config.copy(predicates = predicates, columns = columns, options = options))
      assert(df.queryExecution.optimizedPlan.toString.contains("JDBCRelation"))
      df
    }

    val all = read()
    assert(all.columns.toSeq == Seq("id", "value"))
    val allRows = all.collect()
    assert(allRows.map(_.getInt(0)).toSet == Set(1, 2))
    val directRows = queryColumn(s"SELECT id, value FROM `$database`.`$table` ORDER BY id") {
      result => (result.getInt(1), result.getString(2))
    }
    assert(directRows == Seq((1, "one"), (2, "two")))
    val charWidth = withDorisConnection { connection =>
      val statement = connection.createStatement()
      try {
        val result = statement.executeQuery(s"SELECT value FROM `$database`.`$table` WHERE 1 = 0")
        try {
          val metadata = result.getMetaData
          if (metadata.getColumnType(1) == java.sql.Types.CHAR) Some(metadata.getPrecision(1))
          else None
        } finally result.close()
      } finally statement.close()
    }
    // Spark pads JDBC CHAR values when scanning, even when Doris declared VARCHAR.
    val expectedRows = directRows.map { case (id, value) =>
      (charWidth.fold(value)(width => value.padTo(width, ' ')), id)
    }.toSet
    assert(allRows.map(row => (row.getString(1), row.getInt(0))).toSet == expectedRows)
    val filtered = read(Seq("id = 1"))
    assert(filtered.columns.toSeq == Seq("id", "value"))
    assert(filtered.collect().map(_.getInt(0)).toSeq == Seq(1))
    Seq(true, false).foreach { force =>
      val originalCase = read(Seq("id = 1"), Seq("id"), force)
      assert(originalCase.columns.toSeq == Seq("id"))
      assert(originalCase.collect().map(_.getInt(0)).toSeq == Seq(1))
    }
    val projected = read(columns = Seq("value", "id"))
    assert(projected.columns.toSeq == Seq("value", "id"))
    assert(projected.collect().map(row => (row.getString(0), row.getInt(1))).toSet ==
      expectedRows)
    Seq(true, false).foreach { force =>
      val combined = read(Seq("trim(value) = 'one'", "id > 0"), Seq("id"), force)
      assert(combined.columns.toSeq == Seq("id"))
      assert(combined.collect().map(_.getInt(0)).toSeq == Seq(1))
      val disjoint = read(Seq("trim(value) = 'one'", "id = 2"), Seq("id"), force)
      assert(disjoint.collect().isEmpty)
    }
    assert(read(Seq("id = 9"), Seq("id")).collect().isEmpty)
    intercept[Exception] { read(Seq("id ="), Seq("id")).collect() }
    intercept[Exception] { read(columns = Seq("missing_column")).collect() }
  }

  test("Doris JDBC preserves duplicate rows and evaluates composed filters") {
    val definition = """(id INT, payload INT NULL, `odd.name` INT) DUPLICATE KEY(id)
      |DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ("replication_num" = "2")""".stripMargin
    val (database, table) = createDorisTable("it_doris_reader_correctness", definition)
    executeDoris(s"INSERT INTO `$database`.`$table` VALUES (1, 11, 101), (1, 11, 101), (2, NULL, 202), (3, 33, 303)")
    val options = Map("doris.fenodes" -> fenodes, "doris.query.port" -> feQueryPort.toString,
      "doris.user" -> "root", "doris.password" -> "")
    val config = InputConfig(enabled = true, path = Some(s"$database.$table"), format = "doris",
      options = options)
    val oracle = queryColumn(s"SELECT id, payload, `odd.name` FROM `$database`.`$table`") { result =>
      val id = result.getInt(1)
      val payload = result.getInt(2)
      val optionalPayload = if (result.wasNull()) None else Some(payload)
      (id, optionalPayload, result.getInt(3))
    }.sortBy(row => (row._1, row._3))
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

    val noPushdown = new DorisReader {
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

    val (emptyDatabase, emptyTable) = createDorisTable("it_doris_reader_empty", definition)
    val empty = read(config.copy(path = Some(s"$emptyDatabase.$emptyTable")))
    assert(empty.columns.toSeq == Seq("id", "payload", "odd.name"))
    assert(empty.collect().isEmpty)
  }
}
