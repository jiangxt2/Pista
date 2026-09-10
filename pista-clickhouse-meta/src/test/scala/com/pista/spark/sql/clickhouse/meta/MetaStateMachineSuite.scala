package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.sql.clickhouse.meta.record.RecordStatus._
import com.pista.spark.sql.clickhouse.meta.record._
import org.scalatest.funsuite.AnyFunSuite

/**
 * MetaStateMachine Unit Test
 *
 * Replace the real database connection with StubRecordManager to test state machine branch logic.
 */
class MetaStateMachineSuite extends AnyFunSuite {

  /** behaviors that are managed in a transactional manner RecordManager stubwithout database connection */
  private class StubRecordManager extends MetaRecordManager(null) {
    var stubRecord: Option[DataRecord] = None
    var createdRecord: Option[DataRecord] = None
    var lastStatusUpdate: Option[DataRecord] = None

    override def searchRecord[T <: Record](record: T): Option[T] = record match {
      case _: DataRecord => stubRecord.asInstanceOf[Option[T]]
      case _             => None
    }

    override def createRecord[T <: Record](record: T): Option[T] = record match {
      case r: DataRecord =>
        val created = r.copy(id = 1)
        createdRecord = Some(created)
        Some(created.asInstanceOf[T])
      case r => Some(r)
    }

    override def updateTaskStatus(record: Record): Boolean = {
      record match {
        case r: DataRecord => lastStatusUpdate = Some(r)
        case _             =>
      }
      true
    }

    override def updateHostAddress(record: DataRecord): Boolean = true
    override def updateDataVolume(record: Record): Boolean = true
    override def setRecordsToNonsense(record: DataInfoRecord): Unit = ()
    override def deleteRecord[T <: Record](record: T): Int = 1
  }

  private def baseRecord = DataRecord(
    dbName = "mydb", tbName = "mytable", rDate = "20260101",
    clusterId = 1, dataIndex = 0, indexSize = 3
  )

  // ===================== verifyDataRecord - overwrite=false =====================

  test("No record found, create a new one and return Some") {
    val stub = new StubRecordManager
    stub.stubRecord = None
    val result = new MetaStateMachine(stub).verifyDataRecord(baseRecord, overwrite = false)
    assert(result.isDefined)
    assert(stub.createdRecord.isDefined)
  }

  test("status=SUCCESS and originalDataVolume>0 skip when and return null None)") {
    val stub = new StubRecordManager
    stub.stubRecord = Some(baseRecord.copy(id = 1, status = SUCCESS_VALUE, originalDataVolume = 100L, insertDataVolume = 100L))
    val result = new MetaStateMachine(stub).verifyDataRecord(baseRecord, overwrite = false)
    assert(result.isEmpty)
  }

  test("status=FAILURE when re-executing (return Some)") {
    val stub = new StubRecordManager
    stub.stubRecord = Some(baseRecord.copy(id = 1, status = FAILURE_VALUE))
    val result = new MetaStateMachine(stub).verifyDataRecord(baseRecord, overwrite = false)
    assert(result.isDefined)
  }

  test("status=INITIAL when re-executed (returns Some)") {
    val stub = new StubRecordManager
    stub.stubRecord = Some(baseRecord.copy(id = 1, status = INITIAL_VALUE))
    val result = new MetaStateMachine(stub).verifyDataRecord(baseRecord, overwrite = false)
    assert(result.isDefined)
  }

  test("status=INITIAL/FAILURE corrected to match the data volume and proceed. SUCCESS skip this record") {
    val stub = new StubRecordManager
    stub.stubRecord = Some(baseRecord.copy(
      id = 1, status = FAILURE_VALUE, originalDataVolume = 200L, insertDataVolume = 200L))
    val result = new MetaStateMachine(stub).verifyDataRecord(baseRecord, overwrite = false)
    assert(result.isEmpty)
    assert(stub.lastStatusUpdate.exists(_.status == SUCCESS_VALUE))
  }

  test("indexSize changes throw exception") {
    val stub = new StubRecordManager
    // Original record: indexSize=3, new request: indexSize=5
    stub.stubRecord = Some(baseRecord.copy(id = 1, indexSize = 3))
    val newRecord = baseRecord.copy(indexSize = 5)
    assertThrows[Exception] {
      new MetaStateMachine(stub).verifyDataRecord(newRecord, overwrite = false)
    }
  }

  // ===================== verifyDataRecord - overwrite=true =====================

  test("overwrite=true resets SUCCESS records to INITIAL and returns Some") {
    val stub = new StubRecordManager
    val existingRecord = baseRecord.copy(id = 1, status = SUCCESS_VALUE, originalDataVolume = 100L)
    stub.stubRecord = Some(existingRecord)
    // Reset and return searchRecords that return records in the INITIAL state
    val resetRecord = existingRecord.copy(status = INITIAL_VALUE)
    var searchCount = 0
    val smartStub = new StubRecordManager {
      override def searchRecord[T <: Record](record: T): Option[T] = {
        searchCount += 1
        // First query returns SUCCESS, second query (after reset) returns INITIAL
        val r = if (searchCount == 1) existingRecord else resetRecord
        Some(r.asInstanceOf[T])
      }
    }
    val result = new MetaStateMachine(smartStub).verifyDataRecord(baseRecord, overwrite = true)
    assert(result.isDefined)
  }

  test("overwrite=true resets RUNNING records for execution") {
    val stub = new StubRecordManager
    val existingRecord = baseRecord.copy(id = 1, status = RUNNING_VALUE)
    stub.stubRecord = Some(existingRecord)
    val smartStub: StubRecordManager = new StubRecordManager {
      var calls = 0
      override def searchRecord[T <: Record](record: T): Option[T] = {
        calls += 1
        val r = if (calls == 1) existingRecord else existingRecord.copy(status = INITIAL_VALUE)
        Some(r.asInstanceOf[T])
      }
    }
    val result = new MetaStateMachine(smartStub).verifyDataRecord(baseRecord, overwrite = true)
    assert(result.isDefined)
  }
}
