package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.doris.meta.record.DorisFERecord
import com.pista.spark.errors.PistaErrors
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging

import java.sql.SQLException

/**
 * Doris FE Node Address Resolver
 *
 * Priority Parsing (User Agreement):
 *   1. Parameter passed in as the fenodes string (highest priority)
 *   2. A doris_fe_info record matching cluster_name (fallback)
 *
 * Results use the comma-separated `host:httpPort` format expected by the Spark Doris Connector.
 * `doris.fenodes` option formats are consistent.
 *
 * Design Explanation:
 * - Connection is injected via constructor instead of being newly created: reusing the metadata connection held by the enclosing scope.
 *   Avoid duplicating PostgreSQL connection lifecycle management within Resolver
 * - returns None when pista-doris-meta is not available on the classpath
 *   As a connection parameter, only parameter mode is available.
 * - is_leader Leader node sorting priority, the first handshake of the Connector matches the Master FE.
 *
 * @param connection Optional metadata connection (if absent, only parameter mode is supported)
 *
 */
class DorisFENodeResolver(connection: Option[DorisMetaConnection]) extends Logging {

  /**
   * Parse FE node address.
   *
   * @param paramFenodes The provided fenodes passed in (highest priority)
   * @param clusterName  cluster name, used for querying doris_fe_info table
   * @return A string like "fe1:8030,fe2:8030"
   * @throws IllegalStateException Throws an exception when no parameter is provided and the table cannot be parsed.
   */
  def resolve(paramFenodes: Option[String], clusterName: String): String = {
    paramFenodes.map(_.trim).filter(_.nonEmpty) match {
      case Some(fenodes) =>
        logDebug(s"[DorisFENodeResolver] source=configuration feCount=${fenodes.split(",").length}")
        fenodes

      case None =>
        logDebug(s"[DorisFENodeResolver] source=metadata clusterRef=${LogRedaction.fingerprint(clusterName)}")
        resolveFromTable(clusterName).getOrElse(
          throw new IllegalStateException(
            s"Cannot resolve Doris FE nodes: parameter not provided and " +
              s"no records in doris_fe_info for clusterRef=${LogRedaction.fingerprint(clusterName)}"))
    }
  }

  /**
   * Query the list of FE nodes from the doris_fe_info table.
   * return leader node first leader nodes preceding in the list, making it easier for Connector handshake hit Master.
   */
  private def resolveFromTable(clusterName: String): Option[String] = {
    val conn = connection.getOrElse {
      logWarning("[DorisFENodeResolver] No metadata connection available")
      return None
    }

    val records = queryFERecords(conn, clusterName)
    if (records.isEmpty) {
      logWarning(s"[DorisFENodeResolver] No FE records found for clusterRef=${LogRedaction.fingerprint(clusterName)}")
      None
    } else {
      val sorted = records.sortBy(r => (!r.isLeader, r.feHost))
      val fenodes = sorted.map(r => s"${r.feHost}:${r.feHttpPort}").mkString(",")
      logDebug(s"[DorisFENodeResolver] source=metadata feCount=${records.size}")
      Some(fenodes)
    }
  }

  /** Query the doris_fe_info table and return all records matching clusterName */
  private[meta] def queryFERecords(
    conn:        DorisMetaConnection,
    clusterName: String
  ): Seq[DorisFERecord] = {
    val sql =
      """
        |SELECT id, cluster_name, fe_host, fe_query_port, fe_http_port,
        |       is_leader, mtimestamp
        |FROM doris_fe_info
        |WHERE cluster_name = ?
        |""".stripMargin

    try {
      val c = conn.getConnection
      try {
        val ps = c.prepareStatement(sql)
        ps.setString(1, clusterName)
        val rs = ps.executeQuery()
        val buf = scala.collection.mutable.ArrayBuffer.empty[DorisFERecord]
        while (rs.next()) {
          buf += DorisFERecord(
            id          = rs.getLong("id"),
            clusterName = rs.getString("cluster_name"),
            feHost      = rs.getString("fe_host"),
            feQueryPort = rs.getInt("fe_query_port"),
            feHttpPort  = rs.getInt("fe_http_port"),
            isLeader    = rs.getBoolean("is_leader"),
            mtimestamp  = rs.getTimestamp("mtimestamp")
          )
        }
        rs.close()
        ps.close()
        buf.toSeq
      } finally c.close()
    } catch {
      case e: Exception =>
        val state = e match {
          case sql: SQLException => Option(sql.getSQLState).getOrElse("unknown")
          case _ => "none"
        }
        val reason = s"Metadata FE lookup failed; clusterRef=${LogRedaction.fingerprint(clusterName)} " +
          s"sqlState=$state error=${LogRedaction.exceptionName(e)}"
        logError(s"[DorisFENodeResolver] stage=metadata.resolve-fe $reason", LogRedaction.sanitizedThrowable(e))
        throw PistaErrors.dorisWriterError(reason, e)
    }
  }
}
