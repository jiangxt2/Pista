package com.pista.spark.sql.test.util

import com.pista.spark.sql.test.container.{ClickHouseMachineRecord, PistaClickHouseContainer, PistaDorisContainer}

import java.io.InputStream
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/** Idempotently initializes the real PostgreSQL schema used by Pista metadata. */
object MetaSchemaInitializer {
  private val ddlFiles = Seq(
    "00_create_common_functions.sql",
    "01_create_cluster_info.sql",
    "02_create_machine_info.sql",
    "03_create_data_info.sql",
    "04_create_data_records.sql",
    "06_create_doris_fe_info.sql",
    "07_create_doris_task_info.sql",
    "08_create_doris_partition_records.sql")

  private val CreateTable: Regex = "(?is)^CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([A-Za-z0-9_\\.]+).*".r
  private val CreateTrigger: Regex = "(?is)^CREATE\\s+TRIGGER\\s+([A-Za-z0-9_]+)\\s+.*?ON\\s+([A-Za-z0-9_]+).*".r
  private val CreateIndex: Regex = "(?is)^CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+([A-Za-z0-9_]+).*".r
  private val AddConstraint: Regex = "(?is)^ALTER\\s+TABLE\\s+([A-Za-z0-9_]+).*?ADD\\s+CONSTRAINT\\s+([A-Za-z0-9_]+).*".r
  private val ClusterInfoSeedPrefix: Regex =
    "(?i)^INSERT\\s+INTO\\s+(?:[A-Za-z0-9_]+\\.)?CLICKHOUSE_CLUSTER_INFO\\b".r

  def initializeClickHouse(jdbcUrl: String,
                            user: String,
                            password: String,
                            container: PistaClickHouseContainer,
                            clusterId: Int = 1): Unit = {
    withTransaction(jdbcUrl, user, password) { connection =>
      initializeSchema(connection)
      ensureCluster(connection)
      replaceMachines(connection, clusterId, container.machineRecords)
    }
  }

  def initializeSchemaOnly(jdbcUrl: String, user: String, password: String): Unit =
    withTransaction(jdbcUrl, user, password) { connection =>
      initializeSchema(connection)
      // The production DDL contains this seed, but it is intentionally skipped
      // together with DROP statements so initialization stays non-destructive.
      ensureCluster(connection)
    }

  def initializeDoris(jdbcUrl: String,
                      user: String,
                      password: String,
                      container: PistaDorisContainer,
                      clusterName: String = "pista_it"): Unit = {
    withTransaction(jdbcUrl, user, password) { connection =>
      initializeSchema(connection)
      val statement = connection.prepareStatement(
        "DELETE FROM doris_fe_info WHERE cluster_name = ?")
      try {
        statement.setString(1, clusterName)
        statement.executeUpdate()
      } finally statement.close()

      val insert = connection.prepareStatement(
        "INSERT INTO doris_fe_info (cluster_name, fe_host, fe_query_port, fe_http_port, is_leader) " +
          "VALUES (?, ?, ?, ?, ?) ON CONFLICT (cluster_name, fe_host) DO UPDATE SET " +
          "fe_query_port = EXCLUDED.fe_query_port, fe_http_port = EXCLUDED.fe_http_port, " +
          "is_leader = EXCLUDED.is_leader")
      try {
        insert.setString(1, clusterName)
        insert.setString(2, container.feHost)
        insert.setInt(3, container.feQueryPort)
        insert.setInt(4, container.feHttpPort)
        insert.setBoolean(5, true)
        insert.executeUpdate()
      } finally insert.close()
    }
  }

  private def initializeSchema(connection: Connection): Unit = {
    execute(connection, "SELECT pg_advisory_xact_lock(hashtext('pista-it-meta-init'))")
    ddlFiles.foreach { file =>
      readResource(file).foreach { statement =>
        if (!isDestructive(statement)) executeIfNeeded(connection, statement)
      }
    }
  }

  private def ensureCluster(connection: Connection): Unit = {
    execute(connection,
      "INSERT INTO clickhouse_cluster_info (cluster_name) " +
        "SELECT 'ck_cluster' WHERE NOT EXISTS " +
        "(SELECT 1 FROM clickhouse_cluster_info WHERE cluster_name = 'ck_cluster')")
  }

  private def replaceMachines(connection: Connection,
                              clusterId: Int,
                              records: Seq[ClickHouseMachineRecord]): Unit = {
    val delete = connection.prepareStatement("DELETE FROM clickhouse_machine_info WHERE cluster_id = ?")
    try {
      delete.setInt(1, clusterId)
      delete.executeUpdate()
    } finally delete.close()

    val insert = connection.prepareStatement(
      "INSERT INTO clickhouse_machine_info " +
        "(cluster_id, shard_num, replica_num, host_address, only_role, is_alive) VALUES (?, ?, ?, ?, ?, ?)")
    try {
      records.foreach { record =>
        insert.setInt(1, clusterId)
        insert.setInt(2, record.shardNum)
        insert.setInt(3, record.replicaNum)
        insert.setString(4, record.hostPort)
        insert.setInt(5, record.onlyRole)
        insert.setInt(6, record.isAlive)
        insert.addBatch()
      }
      insert.executeBatch()
    }
    finally insert.close()
  }

  private def executeIfNeeded(connection: Connection, sql: String): Unit = sql match {
    case CreateTable(table) if tableExists(connection, table) =>
    case CreateTrigger(trigger, table) if triggerExists(connection, trigger, table) =>
    case CreateIndex(index) if indexExists(connection, index) =>
    case AddConstraint(_, constraint) if constraintExists(connection, constraint) =>
    case statement if ClusterInfoSeedPrefix.findPrefixOf(statement.trim).nonEmpty =>
    case statement => execute(connection, statement)
  }

  private def isDestructive(sql: String): Boolean =
    sql.trim.toUpperCase.startsWith("DROP ")

  private def tableExists(connection: Connection, table: String): Boolean =
    scalar(connection, "SELECT to_regclass(?)", table).nonEmpty

  private def triggerExists(connection: Connection, trigger: String, table: String): Boolean =
    scalar(connection,
      "SELECT 1 FROM pg_trigger WHERE tgname = ? AND tgrelid = ?::regclass AND NOT tgisinternal",
      trigger, table).nonEmpty

  private def indexExists(connection: Connection, index: String): Boolean =
    scalar(connection, "SELECT 1 FROM pg_class WHERE relname = ? AND relkind = 'i'", index).nonEmpty

  private def constraintExists(connection: Connection, constraint: String): Boolean =
    scalar(connection, "SELECT 1 FROM pg_constraint WHERE conname = ?", constraint).nonEmpty

  private def scalar(connection: Connection, sql: String, params: String*): Option[String] = {
    val statement = connection.prepareStatement(sql)
    try {
      params.zipWithIndex.foreach { case (value, index) => statement.setString(index + 1, value) }
      val result = statement.executeQuery()
      try if (result.next()) Option(result.getString(1)) else None
      finally result.close()
    } finally statement.close()
  }

  private def execute(connection: Connection, sql: String): Unit = {
    val statement = connection.createStatement()
    try statement.execute(sql)
    finally statement.close()
  }

  private def withTransaction[T](url: String, user: String, password: String)(f: Connection => T): T = {
    val connection = DriverManager.getConnection(url, user, password)
    connection.setAutoCommit(false)
    try {
      val result = f(connection)
      connection.commit()
      result
    } catch {
      case t: Throwable =>
        try connection.rollback() catch { case _: Throwable => () }
        throw t
    } finally connection.close()
  }

  private def readResource(file: String): Seq[String] = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(s"postgre/$file"))
      .getOrElse(throw new IllegalStateException(s"Missing PostgreSQL DDL resource: $file"))
    try splitStatements(stream)
    finally stream.close()
  }

  /** Split SQL while preserving PostgreSQL dollar-quoted function bodies. */
  private def splitStatements(stream: InputStream): Seq[String] = {
    val sql = scala.io.Source.fromInputStream(stream, "UTF-8").mkString
    val statements = ListBuffer.empty[String]
    val current = new StringBuilder
    var quote: Char = 0
    var dollarQuote: String = null
    var index = 0

    while (index < sql.length) {
      if (dollarQuote != null && sql.startsWith(dollarQuote, index)) {
        current.append(dollarQuote)
        index += dollarQuote.length
        dollarQuote = null
      } else if (dollarQuote == null && quote == 0 && sql.startsWith("--", index)) {
        val end = sql.indexOf('\n', index)
        index = if (end < 0) sql.length else end
      } else if (dollarQuote == null && quote == 0 && sql.charAt(index) == '$') {
        val end = sql.indexOf('$', index + 1)
        if (end > index) {
          dollarQuote = sql.substring(index, end + 1)
          current.append(dollarQuote)
          index = end + 1
        } else {
          current.append(sql.charAt(index)); index += 1
        }
      } else if (dollarQuote == null && sql.charAt(index) == '\'' && quote != '"') {
        quote = if (quote == '\'') 0 else '\''
        current.append(sql.charAt(index)); index += 1
      } else if (dollarQuote == null && sql.charAt(index) == '"' && quote != '\'') {
        quote = if (quote == '"') 0 else '"'
        current.append(sql.charAt(index)); index += 1
      } else if (dollarQuote == null && quote == 0 && sql.charAt(index) == ';') {
        if (current.toString.trim.nonEmpty) statements += current.toString.trim
        current.clear(); index += 1
      } else {
        current.append(sql.charAt(index)); index += 1
      }
    }
    if (current.toString.trim.nonEmpty) statements += current.toString.trim
    statements.toSeq
  }
}
