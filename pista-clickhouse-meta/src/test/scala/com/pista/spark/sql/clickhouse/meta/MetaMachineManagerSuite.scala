package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.sql.clickhouse.meta.record.{DataInfoRecord, DataRecord, Record}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

/**
 * MetaMachineManager Unit Test
 *
 * Replace the real database connection with StubMetaManager to test the machine pool management logic.
 */
class MetaMachineManagerSuite extends AnyFunSuite {

  /** MetaManager stub for behavior, independent of database connection */
  private class StubMetaManager(
    machinesByCluster: Map[Int, List[String]],
    usedMachinesResponse: List[String] = Nil
  ) extends MetaManager(null) {

    override def getMachinesByClusterId(clusterId: Int): List[String] =
      machinesByCluster.getOrElse(clusterId, Nil)

    override def getAlreadyUsedMachines(record: DataInfoRecord): List[String] =
      usedMachinesResponse

    // Other methods are not concerned with; provide default implementation
    override def getClusterIdByName(clusterName: String): Int = 1
    override def getBackupMachine(hostAddress: String, clusterId: Int): String = "backup"
    override def verifyDataInfoRecord(record: DataInfoRecord, overwrite: Boolean): Option[DataInfoRecord] = None
    override def verifyDataRecord(record: DataRecord, overwrite: Boolean): Option[DataRecord] = None
    override def searchRecord(record: DataRecord): Option[DataRecord] = None
    override def searchRecord(record: DataInfoRecord): Option[DataInfoRecord] = None
    override def updateTaskStatus(record: Record): Boolean = true
    override def updateHostAddress(record: DataRecord): Boolean = true
    override def updateDataVolume(record: Record): Boolean = true
    override def setRecordsToNonsense(record: DataInfoRecord): Unit = ()
    override def sumDataVolume(record: DataInfoRecord): Long = 0L
  }

  private def baseDataInfoRecord = DataInfoRecord(
    dbName = "mydb", tbName = "mytable", rDate = "20260101",
    clusterId = 1, indexSize = 3
  )

  // ===================== init() Base Functions =====================

  test("init() return the size of the machine pool, not exceeding the number of requests") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3", "host4")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    val result = mgr.init(requestedMachineCount = 3)

    assert(result == 3)
    // totalCount represents the number of remaining machines in the machine pool, not the number of shards returned.
    // Because no machines were filtered, there are still 4 machines in the pool.
    assert(mgr.totalCount == 4)
  }

  test("init() requests exceed the available machine count, returns the available machine count") {
    val clusterId = 1
    val machines = List("host1", "host2")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    val result = mgr.init(requestedMachineCount = 5)

    assert(result == 2)
    // totalCount is the number of remaining machines in the machine pool, which is 2.
    assert(mgr.totalCount == 2)
  }

  test("init() when passing usedMachines filters the machine pool") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3", "host4")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    val result = mgr.init(requestedMachineCount = 5, usedMachines = Set("host1", "host2"))

    // host1, host2 are filtered, leaving host3, host4.
    assert(result == 2)
    assert(mgr.totalCount == 2)
  }

  // ===================== chooseOneMachine() =====================

  test("chooseOneMachine() Assign machines in order (FIFO)") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    mgr.init(requestedMachineCount = 3)

    val chosen = mutable.ListBuffer[String]()
    chosen += mgr.chooseOneMachine()
    chosen += mgr.chooseOneMachine()
    chosen += mgr.chooseOneMachine()

    assert(chosen.length == 3)
    assert(chosen.toSet == machines.toSet) // Content matches, order may differ due to shuffle
  }

  test("chooseOneMachine() throws an exception when the machine pool is exhausted") {
    val clusterId = 1
    val machines = List("host1", "host2")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    mgr.init(requestedMachineCount = 2)
    mgr.chooseOneMachine()
    mgr.chooseOneMachine()

    assertThrows[Exception] {
      mgr.chooseOneMachine()
    }
  }

  // ===================== removeFromPool() =====================

  test("removeFromPool() Removes machines used from the machine pool") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3", "host4")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    mgr.init(requestedMachineCount = 4)
    assert(mgr.totalCount == 4)

    mgr.removeFromPool(Set("host1", "host2"))

    assert(mgr.totalCount == 2)
    // The remaining machines should be host3 and host4 (the order may differ).
    val remaining = Seq(mgr.chooseOneMachine(), mgr.chooseOneMachine())
    assert(remaining.toSet == Set("host3", "host4"))
  }

  test("removeFromPool() Empty set does not affect the machine pool.") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    mgr.init(requestedMachineCount = 3)
    mgr.removeFromPool(Set.empty)

    assert(mgr.totalCount == 3)
  }

  test("removeFromPool() Removes allocated machines without affecting those polled out") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3", "host4")
    val stub = new StubMetaManager(Map(clusterId -> machines))
    val mgr = new MetaMachineManager(stub, clusterId)

    mgr.init(requestedMachineCount = 4)

    // Distribute one machine first
    val assigned1 = mgr.chooseOneMachine()
    assert(mgr.totalCount == 3)

    // Remove assigned machines (should not remove assigned machines)
    mgr.removeFromPool(Set(assigned1))
    assert(mgr.totalCount == 3) // still three in the pool 3 instance

    // Continue to distribute remaining machines.
    val assigned2 = mgr.chooseOneMachine()
    val assigned3 = mgr.chooseOneMachine()
    val assigned4 = mgr.chooseOneMachine()

    assert(assigned2 != assigned1)
    assert(assigned3 != assigned1)
    assert(assigned4 != assigned1)
  }

  test("removeFromPool() removed from the pool differs from initializing used machines. init(usedMachines) differences") {
    val clusterId = 1
    val machines = List("host1", "host2", "host3", "host4")
    val stub = new StubMetaManager(Map(clusterId -> machines))

    // method in mode 1 passes the usedMachines during initialization, resulting in a smaller return value. 1: init When passed in usedMachinesthe return value will be smaller
    val mgr1 = new MetaMachineManager(stub, clusterId)
    val count1 = mgr1.init(4, Set("host1", "host2"))
    assert(count1 == 2) // shard The shard count is reduced.

    // Method 2: omit used machines from init, then call removeFromPool without changing shard count.
    val mgr2 = new MetaMachineManager(stub, clusterId)
    val count2 = mgr2.init(4) // No filter applied
    assert(count2 == 4) // shard Number of shards unchanged
    mgr2.removeFromPool(Set("host1", "host2")) // Further filtering
    assert(mgr2.totalCount == 2) // the number of shards remains at 4, but the number of tasks has shrunk. shard remain four 4
  }
}
