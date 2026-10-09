package com.pista.spark.sql.doris.meta

import com.pista.spark.errors.{PistaDorisException, PistaErrors}
import com.pista.spark.sql.doris.meta.record.DorisRecordStatus
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging

import java.sql.{Connection, SQLException}
import scala.util.Using
import scala.util.control.NonFatal

object DorisMetaManager {
  val TaskIdMaxChars: Int = 256

  /** PostgreSQL varchar limits count Unicode characters, not UTF-8 bytes. */
  def characterCount(value: String): Int =
    Option(value).map(text => text.codePointCount(0, text.length)).getOrElse(0)
}

/** Task-level PostgreSQL state; a configured store never degrades on an error. */
class DorisMetaManager(
  connection: DorisMetaConnection,
  metaConf: DorisMetaConf
) extends Logging {
  import DorisMetaManager.characterCount

  def isSucceeded(taskId: String): Boolean =
    metadataOperation("check-success", taskId) { conn =>
      Using.resource(conn.prepareStatement(
        "SELECT 1 FROM doris_task_info WHERE task_id = ? AND status = ?")) { statement =>
        statement.setString(1, taskId)
        statement.setInt(2, DorisRecordStatus.SUCCESS.id)
        Using.resource(statement.executeQuery())(_.next())
      }
    }

  def hasAnyRecord(taskId: String): Boolean =
    metadataOperation("check-history", taskId) { conn =>
      Using.resource(conn.prepareStatement(
        "SELECT 1 FROM doris_task_info WHERE task_id = ?")) { statement =>
        statement.setString(1, taskId)
        Using.resource(statement.executeQuery())(_.next())
      }
    }

  def upsertRunning(
    taskId: String,
    database: String,
    table: String,
    partitionDate: String,
    writeMode: String,
    sourceTable: Option[String],
    partitionFilter: Option[String]
  ): Unit = {
    val fields = Seq(
      ("task_id", taskId, DorisMetaManager.TaskIdMaxChars),
      ("cluster_name", metaConf.clusterName, 128),
      ("dbname", database, 128),
      ("tbname", table, 128),
      ("write_mode", writeMode, 32),
      ("source_table", sourceTable.orNull, 256))
    val lengths = (fields.map { case (name, value, _) => s"${name}Chars=${characterCount(value)}" } ++
      Seq(s"rdateChars=${characterCount(partitionDate)}",
        s"partitionFilterChars=${characterCount(partitionFilter.orNull)}")).mkString(" ")

    metadataOperation("running", taskId, lengths) { conn =>
      fields.foreach { case (name, value, limit) => checkLength(name, value, limit) }
      checkPartitionColumns(conn, partitionDate, partitionFilter)
      val sql = s"""
        |INSERT INTO doris_task_info (
        |  task_id, cluster_name, dbname, tbname, rdate,
        |  write_mode, source_table, partition_filter,
        |  expected_rows, written_rows, status, start_time
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, -1, -1, ${DorisRecordStatus.RUNNING.id}, NOW())
        |ON CONFLICT (task_id) DO UPDATE SET
        |  status = ${DorisRecordStatus.RUNNING.id}, start_time = NOW(), end_time = NULL,
        |  written_rows = -1, rdate = EXCLUDED.rdate, write_mode = EXCLUDED.write_mode,
        |  source_table = EXCLUDED.source_table, partition_filter = EXCLUDED.partition_filter
        |""".stripMargin
      Using.resource(conn.prepareStatement(sql)) { statement =>
        statement.setString(1, taskId)
        statement.setString(2, metaConf.clusterName)
        statement.setString(3, database)
        statement.setString(4, table)
        statement.setString(5, partitionDate)
        statement.setString(6, writeMode)
        statement.setString(7, sourceTable.getOrElse(""))
        statement.setString(8, partitionFilter.orNull)
        requireOneRow(statement.executeUpdate(), "RUNNING")
      }
    }
  }

  def markSuccess(taskId: String, rowCount: Long): Unit =
    metadataOperation("success", taskId, s"writtenRows=$rowCount") { conn =>
      Using.resource(conn.prepareStatement(
        "UPDATE doris_task_info SET written_rows = ?, status = ?, end_time = NOW() WHERE task_id = ?")) { statement =>
        statement.setLong(1, rowCount)
        statement.setInt(2, DorisRecordStatus.SUCCESS.id)
        statement.setString(3, taskId)
        requireOneRow(statement.executeUpdate(), "SUCCESS")
      }
    }

  def markFailure(taskId: String, errorMessage: String): Unit =
    metadataOperation("failure", taskId) { conn =>
      // The original error belongs to the caller; do not persist or log its potentially sensitive message.
      Using.resource(conn.prepareStatement(
        "UPDATE doris_task_info SET status = ?, end_time = NOW() WHERE task_id = ?")) { statement =>
        statement.setInt(1, DorisRecordStatus.FAILURE.id)
        statement.setString(2, taskId)
        requireOneRow(statement.executeUpdate(), "FAILURE")
      }
    }

  private def checkPartitionColumns(
    conn: Connection,
    partitionDate: String,
    partitionFilter: Option[String]
  ): Unit = {
    val values = Map("rdate" -> partitionDate, "partition_filter" -> partitionFilter.orNull)
    Using.resource(conn.prepareStatement(
      "SELECT column_name, character_maximum_length FROM information_schema.columns " +
        "WHERE table_schema = 'public' AND table_name = 'doris_task_info' " +
        "AND column_name IN ('rdate', 'partition_filter')")) { statement =>
      Using.resource(statement.executeQuery()) { result =>
        var found = Set.empty[String]
        while (result.next()) {
          val column = result.getString("column_name")
          found += column
          val limit = result.getInt("character_maximum_length")
          if (!result.wasNull() && characterCount(values(column)) > limit)
            throw PistaErrors.dorisWriterError(
              s"Metadata column $column has limit=$limit, requestedChars=${characterCount(values(column))}; " +
                "apply sqls/postgre/migrations/doris_task_metadata_text.sql before retrying")
        }
        if (found != values.keySet)
          throw PistaErrors.dorisWriterError(
            "Metadata schema is missing or inaccessible: public.doris_task_info requires rdate and partition_filter")
      }
    }
  }

  private def checkLength(column: String, value: String, limit: Int): Unit =
    if (characterCount(value) > limit)
      throw PistaErrors.dorisWriterError(
        s"Metadata field $column exceeds limit=$limit, requestedChars=${characterCount(value)}")

  private def requireOneRow(updated: Int, state: String): Unit =
    if (updated != 1)
      throw PistaErrors.dorisWriterError(
        s"Metadata $state transition affected $updated rows; expected exactly one registered task")

  private def metadataOperation[T](stage: String, taskId: String, details: String = "")(
    operation: Connection => T
  ): T = {
    val taskRef = LogRedaction.fingerprint(taskId)
    val started = System.nanoTime()
    try {
      val result = Using.resource(connection.getConnection)(operation)
      logDebug(s"[DorisMetaManager] stage=metadata.$stage taskRef=$taskRef " +
        s"elapsedMs=${(System.nanoTime() - started) / 1000000} $details")
      result
    } catch {
      case NonFatal(error) =>
        val sqlState = Iterator.iterate(error)(_.getCause).takeWhile(_ != null)
          .collectFirst { case sql: SQLException => Option(sql.getSQLState).getOrElse("unknown") }
          .getOrElse("none")
        val failure = error match {
          case known: PistaDorisException => known
          case other => PistaErrors.dorisWriterError(
            s"Metadata stage=$stage failed; taskRef=$taskRef sqlState=$sqlState " +
              s"error=${LogRedaction.exceptionName(other)}; check the configured PostgreSQL store and metadata schema", other)
        }
        logError(s"[DorisMetaManager] stage=metadata.$stage taskRef=$taskRef sqlState=$sqlState " +
          s"elapsedMs=${(System.nanoTime() - started) / 1000000} $details " +
          s"reason=${failure.getMessageParameters.get("reason")}", LogRedaction.sanitizedThrowable(error))
        throw failure
    }
  }

  def close(): Unit = connection.close()
}
