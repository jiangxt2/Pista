package com.pista.spark.sql.clickhouse.meta.record

object RecordStatus {
  /** Initial State (Not Executed) */
  val INITIAL_VALUE: Int = 0
  /** in progress */
  val RUNNING_VALUE: Int = 2
  /** success */
  val SUCCESS_VALUE: Int = 1
  /** Failure */
  val FAILURE_VALUE: Int = -1
  /** Overwrite Temporary State (Used for Resetting Existing Records During Overwrite Writes) */
  val NONSENSE_VALUE: Int = -2
}
