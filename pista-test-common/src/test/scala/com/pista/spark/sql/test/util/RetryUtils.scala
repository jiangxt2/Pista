package com.pista.spark.sql.test.util

import scala.concurrent.duration.FiniteDuration

object RetryUtils {
  def retry[T](attempts: Int, delay: FiniteDuration)(block: => T): T = {
    require(attempts > 0, "attempts must be positive")
    var last: Throwable = null
    var attempt = 0
    while (attempt < attempts) {
      try return block
      catch {
        case t: Throwable =>
          last = t
          if (attempt + 1 < attempts) Thread.sleep(delay.toMillis)
      }
      attempt += 1
    }
    throw new IllegalStateException(s"Operation failed after $attempts attempts", last)
  }
}
