package com.pista.spark.sql.doris.meta.record

/**
 * Doris metadata state enumeration
 *
 */
object DorisRecordStatus extends Enumeration {
  type DorisRecordStatus = Value

  /** in progress */
  val RUNNING: DorisRecordStatus = Value(0)

  /** success */
  val SUCCESS: DorisRecordStatus = Value(1)

  /** Failure */
  val FAILURE: DorisRecordStatus = Value(2)

  /** Cancelled **/
  val CANCELLED: DorisRecordStatus = Value(3)

  /**
   * Convert integer values to state enumeration
   */
  def fromInt(code: Int): DorisRecordStatus = values.find(_.id == code).getOrElse(
    throw new IllegalArgumentException(s"Unknown status code: $code")
  )
}
