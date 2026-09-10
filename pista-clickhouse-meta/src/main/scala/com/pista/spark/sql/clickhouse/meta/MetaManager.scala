package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.sql.clickhouse.meta.record._
import org.apache.spark.internal.Logging

/**
 * ClickHouse cluster metadata management
 *
 * Re-structured internal architecture:
 * - MetaRecordManager: SQL GENERATION + CRUD OPERATIONS
 * - MetaStateMachine: state transition logic
 * - MetaMachineManager: machine selection and fault tolerance (existing, reuse)
 *
 * Extern API remains unchanged; consumers do not need to perceive internal sharding.
 *
 * @param conn connection
 */
class MetaManager(conn: MetaConnection) extends Logging with AutoCloseable {

  private val recordManager = new MetaRecordManager(conn)
  private val stateMachine = new MetaStateMachine(recordManager)

  // ==================== Public API (Exposed to Submodule) ====================

  def verifyDataRecord(record: DataRecord, overwrite: Boolean): Option[DataRecord] =
    synchronized(stateMachine.verifyDataRecord(record, overwrite))

  def verifyDataInfoRecord(record: DataInfoRecord, overwrite: Boolean): Option[DataInfoRecord] =
    synchronized(stateMachine.verifyDataInfoRecord(record, overwrite))

  def setRecordsToNonsense(record: DataInfoRecord): Unit =
    synchronized(recordManager.setRecordsToNonsense(record))

  def updateHostAddress(record: DataRecord): Boolean =
    synchronized(recordManager.updateHostAddress(record))

  def updateTaskStatus(record: Record): Boolean =
    synchronized(recordManager.updateTaskStatus(record))

  def updateDataVolume(record: Record): Boolean =
    synchronized(recordManager.updateDataVolume(record))

  def sumDataVolume(record: DataInfoRecord): Long =
    synchronized(recordManager.sumDataVolume(record))

  def searchRecord(record: DataInfoRecord): Option[DataInfoRecord] =
    synchronized(recordManager.searchRecord(record))

  def searchRecord(record: DataRecord): Option[DataRecord] =
    synchronized(recordManager.searchRecord(record))

  def getClusterIdByName(clusterName: String): Int =
    synchronized(recordManager.getClusterIdByName(clusterName))

  def getMachinesByClusterId(clusterId: Int): List[String] =
    synchronized(recordManager.getMachinesByClusterId(clusterId))

  def getAlreadyUsedMachines(record: DataInfoRecord): List[String] =
    synchronized(recordManager.getAlreadyUsedMachines(record))

  def getBackupMachine(hostAddress: String, clusterId: Int): String =
    synchronized(recordManager.getBackupMachine(hostAddress, clusterId))

  override def close(): Unit = synchronized(conn.close())
}
