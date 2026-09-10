package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.clickhouse.meta.record.RecordStatus._
import com.pista.spark.sql.clickhouse.meta.record._
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging

import java.sql.{PreparedStatement, ResultSet}
import scala.collection.mutable.ListBuffer

/**
 * Metadata record CRUD management*
 *
 * Responsibilities include generating SQL and performing basic CRUD operations.SQL generation + basic CRUD operations
 * No state machine logic (handled by MetaStateMachine)
 *
 * @param conn connection
 */
class MetaRecordManager(conn: MetaConnection) extends Logging {

  private val TABLE_CLUSTER_INFO  = "clickhouse_cluster_info"
  private val TABLE_MACHINE_INFO  = "clickhouse_machine_info"
  private val TABLE_DATA_RECORDS  = "clickhouse_data_records"
  private val TABLE_DATA_INFO     = "clickhouse_data_info"

  // ==================== SQL generate (by record type) Record Type splitting) ====================

  private def dataRecordSQL(r: DataRecord, op: RecordOperation.Value): (String, Seq[Any]) = op match {
    case RecordOperation.CREATE_RECORD =>
      (s"INSERT INTO $TABLE_DATA_RECORDS " +
        s"(dbname, tbname, rdate, cluster_id, data_index, index_size, " +
        s"original_data_volume, insert_data_volume, host_address, status) " +
        s"VALUES (?, ?, ?, ${r.clusterId}, ${r.dataIndex}, ${r.indexSize}, " +
        s"${r.originalDataVolume}, ${r.insertDataVolume}, ?, $INITIAL_VALUE)",
       Seq(r.dbName, r.tbName, r.rDate, r.hostAddress))

    case RecordOperation.SEARCH_RECORD if r.id == 0 =>
      (s"SELECT id, dbname, tbname, rdate, cluster_id, data_index, index_size, " +
        s"original_data_volume, insert_data_volume, host_address, status " +
        s"FROM $TABLE_DATA_RECORDS " +
        s"WHERE dbname = ? AND tbname = ? AND rdate = ? " +
        s"AND cluster_id = ${r.clusterId} AND data_index = ${r.dataIndex} AND index_size = ${r.indexSize} " +
        s"AND status IN ($FAILURE_VALUE, $INITIAL_VALUE, $SUCCESS_VALUE, $RUNNING_VALUE)",
       Seq(r.dbName, r.tbName, r.rDate))

    case RecordOperation.SEARCH_RECORD =>
      (s"SELECT id, dbname, tbname, rdate, cluster_id, data_index, index_size, " +
        s"original_data_volume, insert_data_volume, host_address, status " +
        s"FROM $TABLE_DATA_RECORDS WHERE id = ${r.id}", Nil)

    case RecordOperation.UPDATE_HOST_ADDRESS =>
      (s"UPDATE $TABLE_DATA_RECORDS SET host_address = ?, status = ${r.status} WHERE id = ${r.id}",
       Seq(r.hostAddress))

    case RecordOperation.UPDATE_TASK_STATUS =>
      (s"UPDATE $TABLE_DATA_RECORDS SET status = ${r.status} WHERE id = ${r.id}", Nil)

    case RecordOperation.UPDATE_DATA_VOLUME =>
      (s"UPDATE $TABLE_DATA_RECORDS " +
        s"SET original_data_volume = ${r.originalDataVolume}, insert_data_volume = ${r.insertDataVolume} " +
        s"WHERE id = ${r.id}", Nil)

    case _ => throw PistaErrors.clickHouseRecordError(s"Operation $op not supported for DataRecord")
  }

  private def machineRecordSQL(r: MachineRecord, op: RecordOperation.Value): (String, Seq[Any]) = op match {
    case RecordOperation.SEARCH_RECORD =>
      (s"SELECT host_address FROM $TABLE_MACHINE_INFO " +
        s"WHERE replica_num = 1 AND cluster_id = ${r.clusterId} AND is_alive = 1", Nil)
    case _ => throw PistaErrors.clickHouseRecordError(s"Operation $op not supported for MachineRecord")
  }

  private def clusterRecordSQL(r: ClusterRecord, op: RecordOperation.Value): (String, Seq[Any]) = op match {
    case RecordOperation.SEARCH_RECORD =>
      (s"SELECT id, cluster_name FROM $TABLE_CLUSTER_INFO WHERE cluster_name = ?",
       Seq(r.clusterName))
    case _ => throw PistaErrors.clickHouseRecordError(s"Operation $op not supported for ClusterRecord")
  }

  private def dataInfoRecordSQL(r: DataInfoRecord, op: RecordOperation.Value): (String, Seq[Any]) = op match {
    case RecordOperation.CREATE_RECORD =>
      (s"INSERT INTO $TABLE_DATA_INFO " +
        s"(dbname, tbname, rdate, cluster_id, index_size, data_volume, status) " +
        s"VALUES (?, ?, ?, ${r.clusterId}, ${r.indexSize}, ${r.dataVolume}, $INITIAL_VALUE)",
       Seq(r.dbName, r.tbName, r.rDate))

    case RecordOperation.SEARCH_RECORD if r.id == 0 =>
      // Without index_size filtering to ensure that old records can be retrieved when indexSize changes
      (s"SELECT id, dbname, tbname, rdate, cluster_id, index_size, data_volume, status " +
        s"FROM $TABLE_DATA_INFO " +
        s"WHERE dbname = ? AND tbname = ? AND rdate = ? " +
        s"AND cluster_id = ${r.clusterId}",
       Seq(r.dbName, r.tbName, r.rDate))

    case RecordOperation.SEARCH_RECORD =>
      (s"SELECT id, dbname, tbname, rdate, cluster_id, index_size, data_volume, status " +
        s"FROM $TABLE_DATA_INFO WHERE id = ${r.id}", Nil)

    case RecordOperation.DELETE_RECORD =>
      (s"DELETE FROM $TABLE_DATA_INFO " +
        s"WHERE dbname = ? AND tbname = ? AND rdate = ? AND cluster_id = ${r.clusterId}",
       Seq(r.dbName, r.tbName, r.rDate))

    case RecordOperation.UPDATE_TASK_STATUS =>
      (s"UPDATE $TABLE_DATA_INFO SET status = ${r.status} WHERE id = ${r.id}", Nil)

    case RecordOperation.UPDATE_DATA_VOLUME =>
      (s"UPDATE $TABLE_DATA_INFO SET data_volume = ${r.dataVolume} WHERE id = ${r.id}", Nil)

    case RecordOperation.SUM_DATA_VOLUME =>
      (s"SELECT sum(insert_data_volume) AS data_volume FROM $TABLE_DATA_RECORDS " +
        s"WHERE dbname = ? AND tbname = ? AND rdate = ? " +
        s"AND cluster_id = ${r.clusterId} AND index_size = ${r.indexSize} AND status = $SUCCESS_VALUE",
       Seq(r.dbName, r.tbName, r.rDate))

    case _ => throw PistaErrors.clickHouseRecordError(s"Operation $op not supported for DataInfoRecord")
  }

  private def sqlFor(record: Record, op: RecordOperation.Value): (String, Seq[Any]) = record match {
    case r: DataRecord     => dataRecordSQL(r, op)
    case r: MachineRecord  => machineRecordSQL(r, op)
    case r: ClusterRecord  => clusterRecordSQL(r, op)
    case r: DataInfoRecord => dataInfoRecordSQL(r, op)
  }

  // ==================== CRUD OPERATIONS ====================

  def createRecord[T <: Record](record: T): Option[T] = {
    val (sql, params) = sqlFor(record, RecordOperation.CREATE_RECORD)
    withPreparedStatement(sql, params)(_.execute())
    searchRecord(record)
  }

  def deleteRecord[T <: Record](record: T): Int = {
    val (sql, params) = sqlFor(record, RecordOperation.DELETE_RECORD)
    executeUpdate(sql, params)
  }

  def searchRecord[T <: Record](record: T): Option[T] = {
    val (sql, params) = sqlFor(record, RecordOperation.SEARCH_RECORD)
    logInfo(s"Search SQL fingerprint: ${LogRedaction.fingerprint(sql)}")
    withPreparedStatement(sql, params) { ps =>
      val rs = ps.executeQuery()
      try {
        if (rs.next()) Option(mapRow(rs, record)) else None
      } finally rs.close()
    }
  }

  def updateTaskStatus(record: Record): Boolean = {
    val (sql, params) = sqlFor(record, RecordOperation.UPDATE_TASK_STATUS)
    executeUpdate(sql, params) >= 0
  }

  def updateDataVolume(record: Record): Boolean = {
    val (sql, params) = sqlFor(record, RecordOperation.UPDATE_DATA_VOLUME)
    executeUpdate(sql, params) >= 0
  }

  def updateHostAddress(record: DataRecord): Boolean = {
    val (sql, params) = dataRecordSQL(record, RecordOperation.UPDATE_HOST_ADDRESS)
    executeUpdate(sql, params) == 1
  }

  def sumDataVolume(record: DataInfoRecord): Long = {
    val (sql, params) = dataInfoRecordSQL(record, RecordOperation.SUM_DATA_VOLUME)
    logInfo(s"[MetaRecordManager] sumDataVolume SQL fingerprint: ${LogRedaction.fingerprint(sql)}")
    logInfo(s"[MetaRecordManager] sumDataVolume params: dbname=${record.dbName}, tbname=${record.tbName}, " +
      s"rdate='${record.rDate}' (length=${record.rDate.length}), cluster_id=${record.clusterId}, index_size=${record.indexSize}")
    val result = withPreparedStatement(sql, params) { ps =>
      val rs = ps.executeQuery()
      try if (rs.next()) rs.getLong(1) else 0L
      finally rs.close()
    }
    logInfo(s"[MetaRecordManager] sumDataVolume result: $result")
    result
  }

  def getClusterIdByName(clusterName: String): Int = {
    val (sql, params) = clusterRecordSQL(ClusterRecord(clusterName = clusterName), RecordOperation.SEARCH_RECORD)
    withPreparedStatement(sql, params) { ps =>
      val rs = ps.executeQuery()
      try {
        if (rs.next()) rs.getInt(1)
        else throw PistaErrors.clickHouseRecordError(s"Cluster $clusterName not found")
      } finally rs.close()
    }
  }

  def getMachinesByClusterId(clusterId: Int): List[String] = {
    val (sql, params) = machineRecordSQL(MachineRecord(clusterId = clusterId), RecordOperation.SEARCH_RECORD)
    selectMachines(sql, params)
  }

  def getAlreadyUsedMachines(record: DataInfoRecord): List[String] = {
    // Filter out machines based on legacy implementation of removeUsedMachines() behavior: exclude all machines that are not NONSENSE.
    // in states INITIAL/FAILURE/RUNNING/SUCCESSto prevent new shards from being assigned to machines that already contain shards
    val sql = s"SELECT DISTINCT host_address FROM $TABLE_DATA_RECORDS " +
      s"WHERE dbname = ? AND tbname = ? AND rdate = ? " +
      s"AND cluster_id = ${record.clusterId} " +
      s"AND status != $NONSENSE_VALUE " +
      s"AND host_address IS NOT NULL AND host_address != ''"
    selectMachines(sql, Seq(record.dbName, record.tbName, record.rDate))
  }

  def getBackupMachine(hostAddress: String, clusterId: Int): String = {
    val sql =
      s"SELECT host_address FROM $TABLE_MACHINE_INFO " +
        s"WHERE cluster_id = $clusterId AND is_alive = 1 AND replica_num IN (1, 2) " +
        s"AND host_address != ? " +
        s"AND shard_num IN (SELECT shard_num FROM $TABLE_MACHINE_INFO WHERE host_address = ?)"
    selectMachines(sql, Seq(hostAddress, hostAddress)).headOption
      .getOrElse(throw PistaErrors.clickHouseRecordError(s"No backup machine found for $hostAddress"))
  }

  /**
   * Set the status of all DataRecord that match the specified condition to NONSENSE (soft delete)
   *
   * Marks old records as obsolete when indexSize changes during an overwrite.
   * NONSENSE records will not be retrieved by the SEARCH_RECORD query, achieving natural isolation.
   */
  def setRecordsToNonsense(record: DataInfoRecord): Unit = {
    val sql =
      s"UPDATE $TABLE_DATA_RECORDS SET status = $NONSENSE_VALUE " +
        s"WHERE dbname = ? AND tbname = ? AND rdate = ? " +
        s"AND cluster_id = ${record.clusterId} AND status != $NONSENSE_VALUE"
    executeUpdate(sql, Seq(record.dbName, record.tbName, record.rDate))
  }

  // ==================== Helper Methods ====================

  private def mapRow[T <: Record](rs: ResultSet, record: T): T = (record match {
    case _: DataRecord => DataRecord(
      id = rs.getInt(1), dbName = rs.getString(2), tbName = rs.getString(3),
      rDate = rs.getString(4), clusterId = rs.getInt(5), dataIndex = rs.getInt(6),
      indexSize = rs.getInt(7), originalDataVolume = rs.getLong(8), insertDataVolume = rs.getLong(9),
      hostAddress = rs.getString(10), status = rs.getInt(11))
    case _: MachineRecord => MachineRecord(
      id = rs.getInt(1), clusterId = rs.getInt(2), shardNum = rs.getInt(3),
      replicaNum = rs.getInt(4), hostAddress = rs.getString(5),
      onlyRole = rs.getInt(6), isAlive = rs.getInt(7))
    case _: ClusterRecord => ClusterRecord(id = rs.getInt(1), clusterName = rs.getString(2))
    case _: DataInfoRecord => DataInfoRecord(
      id = rs.getInt(1), dbName = rs.getString(2), tbName = rs.getString(3),
      rDate = rs.getString(4), clusterId = rs.getInt(5), indexSize = rs.getInt(6),
      dataVolume = rs.getLong(7), status = rs.getInt(8))
  }).asInstanceOf[T]

  private def selectMachines(sql: String, params: Seq[Any] = Nil): List[String] =
    withPreparedStatement(sql, params) { ps =>
      logInfo(s"Select-machines SQL fingerprint: ${LogRedaction.fingerprint(sql)}")
      val rs = ps.executeQuery()
      try {
        val buf = ListBuffer.empty[String]
        while (rs.next()) buf += rs.getString(1)
        buf.toList
      } finally rs.close()
    }

  private def executeUpdate(sql: String, params: Seq[Any] = Nil): Int =
    withPreparedStatement(sql, params) { ps =>
      logInfo(s"Update SQL fingerprint: ${LogRedaction.fingerprint(sql)}")
      ps.executeUpdate()
    }

  private def withPreparedStatement[A](sql: String, params: Seq[Any] = Nil)(f: PreparedStatement => A): A = {
    val ps = conn.getConnection.prepareStatement(sql)
    var primaryEx: Throwable = null
    try {
      params.zipWithIndex.foreach { case (v, i) =>
        v match {
          case s: String  => ps.setString(i + 1, s)
          case n: Int     => ps.setInt(i + 1, n)
          case n: Long    => ps.setLong(i + 1, n)
          case b: Boolean => ps.setBoolean(i + 1, b)
          case d: Double  => ps.setDouble(i + 1, d)
          case other =>
            throw PistaErrors.clickHouseRecordError(
              s"Unsupported parameter type: ${other.getClass.getName} at index ${i + 1}")
        }
      }
      f(ps)
    } catch {
      case e: Exception =>
        primaryEx = PistaErrors.clickHouseRecordError(e.getMessage, e)
        throw primaryEx
    } finally {
      try ps.close()
      catch { case e: Exception => if (primaryEx != null) primaryEx.addSuppressed(e) else throw e }
    }
  }
}
