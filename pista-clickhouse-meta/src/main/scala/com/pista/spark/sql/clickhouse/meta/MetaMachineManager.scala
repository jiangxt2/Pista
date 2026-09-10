package com.pista.spark.sql.clickhouse.meta

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.clickhouse.meta.record.DataInfoRecord
import org.apache.spark.internal.Logging

import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.Random

/**
 * ClickHouse machine selection and fault tolerance management
 *
 * Distinction from original ClickHouseMachineUtils (object, singleton global state):
 * - class not objectthrough constructor dependency injection MetaManager
 * - machine list deferred loading, explicit initialization via init()
 * - Exclude used machines (when overwriting is disabled)
 *
 * @param metaManager metaManager
 * @param clusterId   target cluster ID
 */
class MetaMachineManager(metaManager: MetaManager, clusterId: Int) extends Logging {

  // Use ConcurrentLinkedQueue to ensure elements are extracted in the order of their shuffle insertion (FIFO),
  // Avoid the natural ordering of ConcurrentSkipListSet cancelling the shuffle effect due to uneven machine distribution
  private var availableMachines: ConcurrentLinkedQueue[String] = _

  /** Initialize the list of available machines (excluding usedMachines), return the number of actually available machines */
  def init(requestedMachineCount: Int, usedMachines: Set[String] = Set.empty): Int = {
    val all = metaManager.getMachinesByClusterId(clusterId)
    logInfo(s"Total available machines in cluster: ${all.size}")
    val shuffled = Random.shuffle(all).filterNot(usedMachines.contains)
    availableMachines = new ConcurrentLinkedQueue[String]()
    shuffled.foreach(availableMachines.offer)

    val actualCount = availableMachines.size()

    if (usedMachines.nonEmpty)
      logInfo(s"After removing used machines: $actualCount available")

    // When the number of requests exceeds the available capacity, automatically adjust and record a warning.
    if (requestedMachineCount > actualCount) {
      logWarning(s"Requested $requestedMachineCount machines but only $actualCount available. " +
                 s"Adjusting machine count to $actualCount. " +
                 s"Please add more machines to clickhouse_machine_info table if needed.")
    }

    Math.min(requestedMachineCount, actualCount)
  }

  /** Total number of available machines (after initialization) */
  def totalCount: Int = availableMachines.size()

  /** atomically extracting and removing one available machine (FIFO, order matches shuffle results)FIFOshuffled results in order shuffle consistent) */
  def chooseOneMachine(): String = {
    val machine = availableMachines.poll()
    if (machine == null)
      throw PistaErrors.clickHouseRecordError("No available ClickHouse machines")
    logInfo(s"Chose machine: $machine")
    machine
  }

  /** Retrieve standby machines in the same shard from the metadata database */
  def getBackupMachine(hostAddress: String): String =
    metaManager.getBackupMachine(hostAddress, clusterId)

  /** List of machines used by the current task so far (excluding those to be reassigned for non-overwrite writes) */
  def getAlreadyUsedMachines(record: DataInfoRecord): List[String] =
    metaManager.getAlreadyUsedMachines(record)

  /**
   * Removes used machines from the loaded pool, preserving removeUsedMachines compatibility semantics.
   *
   * Difference from init(requestedCount, usedMachines):
   * - init Initializes filtering, returning which affects the number of shards.
   * -removeFromPool is executed after initialization. It removes machines from the pool, but does not affect the number of shards that have been determined.
   *
   * Scenario: When overwriting is disabled, first determine the number of shards, then exclude historical machines that have been used, to avoid shrinking the number of shards.
   */
  def removeFromPool(usedMachines: Set[String]): Unit = {
    if (usedMachines.isEmpty) return

    // ConcurrentLinkedQueue does not support batch removal; it requires a traversal to rebuild.
    val remaining = new ConcurrentLinkedQueue[String]()
    var removedCount = 0
    var machine = availableMachines.poll()
    while (machine != null) {
      if (usedMachines.contains(machine)) {
        removedCount += 1
      } else {
        remaining.offer(machine)
      }
      machine = availableMachines.poll()
    }
    availableMachines = remaining

    logInfo(s"Removed $removedCount used machines from pool, ${availableMachines.size()} remaining")
    if (removedCount > 0) {
      logInfo(s"Removed machines: ${usedMachines.mkString(", ")}")
    }
  }
}
