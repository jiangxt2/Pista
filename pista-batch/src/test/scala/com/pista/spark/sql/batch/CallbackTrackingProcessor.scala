package com.pista.spark.sql.batch

import com.pista.spark.sql.execution.processor.{DataProcessor, ProcessContext}
import org.apache.spark.sql.DataFrame

import java.util.concurrent.atomic.AtomicInteger

/**
 */
object CallbackTrackingProcessor {
  val processCount  = new AtomicInteger(0)
  val callbackCount = new AtomicInteger(0)

  def reset(): Unit = {
    processCount.set(0)
    callbackCount.set(0)
  }
}

class CallbackTrackingProcessor extends DataProcessor {
  override def name: String = "CallbackTrackingProcessor"

  override def process(df: DataFrame, context: ProcessContext): DataFrame = {
    CallbackTrackingProcessor.processCount.incrementAndGet()
    context.postActionCallbacks += { () =>
      CallbackTrackingProcessor.callbackCount.incrementAndGet()
    }
    df
  }
}
