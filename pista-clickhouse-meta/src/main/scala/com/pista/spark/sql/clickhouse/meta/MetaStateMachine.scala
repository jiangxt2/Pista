package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.clickhouse.meta.record.RecordStatus._
import com.pista.spark.sql.clickhouse.meta.record._
import org.apache.spark.internal.Logging

/**
 * MetadataStateMachine manages*
 *
 * Owns state-transition logic: read, decide, update, and return.
 * depending on MetaRecordManager under the hood CRUD operations
 *
 * @param recordManager record manager
 */
class MetaStateMachine(recordManager: MetaRecordManager) extends Logging {

  /**
   * Validate DataRecord to determine whether a shard task needs to be executed and which machine it should be assigned to.
   *
   * Return semantics:
   * - Some(record): Need to execute this sharded task
   * - None: Skip this shard (if successfully matched and data size matches)
   *
   * overwrite = false:
   * - SUCCESS status and data volume > 0 → Skip (return None)
   * - INITIAL/FAILURE status → execution
   *
   * overwrite = true:
   * - Any state is reset to INITIAL and executed
   *
   * @param record A initialized DataRecord
   * @param overwrite Whether writing in overwrite mode
   * @return Verified DataRecord (with id and hostAddress), or None (skipped execution)
   */
  def verifyDataRecord(record: DataRecord, overwrite: Boolean): Option[DataRecord] = {
    // Overwrite mode with row-by-row processing (each shard is individually checked and reset to INITIAL)
    // With the global batch NONSENSE functionality equivalent to the legacy implementation, but implemented differently.
    logInfo(s"Verify DataRecord: $record, overwrite=$overwrite")
    val found = recordManager.searchRecord(record)
    if (found.nonEmpty) {
      val r = found.get
      if (r.indexSize != record.indexSize)
        throw PistaErrors.clickHouseRecordError(
          s"IndexSize changed from ${r.indexSize} to ${record.indexSize}. " +
          "Set spark.pista.clickhouse.overwrite=true to force overwrite.")
      r.status match {
        case INITIAL_VALUE | FAILURE_VALUE
          if r.originalDataVolume == r.insertDataVolume &&
            r.insertDataVolume != 0 && !overwrite =>
          // Data matched, exiting outside of task due to unsynchronized state is corrected to successful.
          val updated = r.copy(status = SUCCESS_VALUE)
          recordManager.updateTaskStatus(updated)
          None // Skip Execution
        case INITIAL_VALUE | FAILURE_VALUE =>
          // Reseed normal retry, return records for subsequent updates
          Some(r)
        case SUCCESS_VALUE if !overwrite && r.originalDataVolume > 0 =>
          // Successfully executed and with data, skip execution
          None
        case RUNNING_VALUE | NONSENSE_VALUE if !overwrite =>
          // Skip in non-overwrite mode to avoid repeated execution
          None
        case SUCCESS_VALUE | RUNNING_VALUE | NONSENSE_VALUE if overwrite =>
          // Overwrite mode: reset to INITIAL and execute
          val addr = if (r.hostAddress.isEmpty) record.hostAddress else r.hostAddress
          val updated = r.copy(hostAddress = addr, status = INITIAL_VALUE)
          recordManager.updateHostAddress(updated)
          recordManager.updateTaskStatus(updated)
          recordManager.searchRecord(updated)
        case SUCCESS_VALUE if r.originalDataVolume == 0 =>
          // Successful but dataless (possibly empty task), retry execution
          val addr = if (r.hostAddress.isEmpty) record.hostAddress else r.hostAddress
          val updated = r.copy(hostAddress = addr, status = INITIAL_VALUE)
          recordManager.updateHostAddress(updated)
          recordManager.updateTaskStatus(updated)
          recordManager.searchRecord(updated)
        case s => throw PistaErrors.clickHouseRecordError(s"Unknown RecordStatus: $s")
      }
    } else {
      recordManager.createRecord(record)
    }
  }

  /**
   * Validate global task information records
   *
   * Core logic:
   * - indexSize equal: reuse the original record, status determines whether to execute
   * - indexSize change: set all old DataRecord to NONSENSE in overwrite mode, delete old DataInfoRecord, and create a new DataInfoRecord.
   *
   * @param record Initialized DataInfoRecord
   * @param overwrite Whether writing in overwrite mode
   * @return Verified DataInfoRecord
   */
  def verifyDataInfoRecord(record: DataInfoRecord, overwrite: Boolean): Option[DataInfoRecord] = {
    logInfo(s"Verify DataInfoRecord: indexSize=${record.indexSize}, overwrite=$overwrite")
    val found = recordManager.searchRecord(record)
    if (found.nonEmpty) {
      val r = found.get
      if (r.indexSize != record.indexSize) {
        // indexSize change: set all old DataRecord to NONSENSE in overwrite mode only
        if (overwrite) {
          recordManager.setRecordsToNonsense(record)
          // Drop old DataInfoRecord, create new DataInfoRecord
          recordManager.deleteRecord(record)
          logInfo(s"IndexSize changed (${r.indexSize} → ${record.indexSize}) in overwrite mode, " +
            "old records marked as NONSENSE, new DataInfoRecord created")
          recordManager.createRecord(record)
        } else {
          // In non-overwrite mode, changes to indexSize: throw an exception and require the user to set overwrite to true
          throw PistaErrors.clickHouseRecordError(
            s"IndexSize changed from ${r.indexSize} to ${record.indexSize}. " +
            "Set spark.pista.clickhouse.overwrite=true to force overwrite.")
        }
      } else {
        // Reuse the existing record when indexSize is unchanged.
        r.status match {
          case INITIAL_VALUE | FAILURE_VALUE =>
            val updated = r.copy(status = INITIAL_VALUE, dataVolume = record.dataVolume)
            recordManager.updateDataVolume(updated)
            recordManager.updateTaskStatus(updated)
            recordManager.searchRecord(updated)
          case SUCCESS_VALUE | RUNNING_VALUE | NONSENSE_VALUE =>
            recordManager.searchRecord(r)
          case s => throw PistaErrors.clickHouseRecordError(s"Unknown RecordStatus: $s")
        }
      }
    } else {
      // No records, create directly.
      recordManager.createRecord(record)
    }
  }

  /**
   * Set the status of all DataRecord that match the specified condition to NONSENSE
   *
   * typically invoked by verifyDataInfoRecord within indexSize called when there is change.
   * Can also be called by the caller before overwriting write.
   */
  def setRecordsToNonsense(record: DataInfoRecord): Unit = {
    recordManager.setRecordsToNonsense(record)
  }
}
