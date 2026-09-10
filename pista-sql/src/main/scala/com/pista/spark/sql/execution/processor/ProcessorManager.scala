package com.pista.spark.sql.execution.processor

import com.pista.spark.errors.PistaErrors
import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
import com.pista.spark.sql.errors.ErrorPolicyResolver
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * processor manager
 *
 * manage and execute the data processor for loading and management*
 *
 */
object ProcessorManager extends Logging {

  /**
   * Load processor from configuration.
   *
   * Configuration:
   * - spark.pista.processor.enabled: Whether processor is enabled (default false)
   * - spark.pista.processor.classes: processor class names listed with commas
   *
   * @param sparkSession Spark Session
   * @param conf Configuration reader (optional, defaults to internal creation if not provided)
   * @return processor list
   */
  def loadProcessors(sparkSession: SparkSession, conf: ConfigReader = null): Seq[DataProcessor] = {
    val effectiveConf = if (conf != null) conf else ConfigReader(sparkSession)

    val enabled = effectiveConf.get(SubmitterConf.PROCESSOR_ENABLED)

    if (!enabled) {
      logInfo("Data processor is disabled")
      Seq.empty
    } else {
      val classNames = effectiveConf.get(SubmitterConf.PROCESSOR_CLASSES).filter(_.nonEmpty)

      if (classNames.isEmpty) {
        logWarning("Data processor is enabled but no processor classes specified")
        Seq.empty
      } else {
        logInfo(s"Loading ${classNames.length} processor(s): ${classNames.mkString(", ")}")

        classNames.flatMap { className =>
          try {
            val clazz = Class.forName(className)
            val processor = clazz.getDeclaredConstructor().newInstance()
              .asInstanceOf[DataProcessor]
            logInfo(s"Successfully loaded processor: ${processor.name} ($className)")
            Some(processor)
          } catch {
            case _: ClassNotFoundException =>
              throw PistaErrors.processorClassNotFoundError(className)
            case e: ClassCastException =>
              throw PistaErrors.processorExecutionError(
                className,
                s"does not implement DataProcessor trait: ${LogRedaction.exceptionName(e)}",
                LogRedaction.sanitizedThrowable(e))
            case e: Exception =>
              throw PistaErrors.processorExecutionError(
                className,
                s"instantiation failed: ${LogRedaction.exceptionName(e)}",
                LogRedaction.sanitizedThrowable(e))
          }
        }
      }
    }
  }

  /**
   * Execution processor chain
   *
   * Executing all processors sequentially, with each processors output serving as the input for the next processor.
   * Decide the behavior on failure based on the error strategy.
   *
   * @param df Input DataFrame
   * @param processors List of processors
   * @param context Context for processing
   * @return Processed DataFrame
   */
  def executeChain(df: DataFrame,
                   processors: Seq[DataProcessor],
                   context: ProcessContext): DataFrame = {
    if (processors.isEmpty) {
      df
    } else {
      logInfo(s"Executing processor chain with ${processors.length} processor(s)")

      val policyResolver = new ErrorPolicyResolver(context.sparkSession)
      val processorPolicy = policyResolver.processorErrorPolicy

      processors.foldLeft(df) { (currentDf, processor) =>
        try {
          logInfo(s"Executing processor: ${processor.name}")
          val startTime = System.currentTimeMillis()
          val result = processor.process(currentDf, context)
          val duration = System.currentTimeMillis() - startTime
          logInfo(s"Processor ${processor.name} completed in ${duration}ms")
          result
        } catch {
          case e: Exception =>
            logError(
              s"Processor ${processor.name} failed with ${LogRedaction.exceptionName(e)}",
              LogRedaction.sanitizedThrowable(e))
            if (processorPolicy.shouldFailFast) {
              throw PistaErrors.processorExecutionError(
                processor.name,
                LogRedaction.exceptionName(e),
                LogRedaction.sanitizedThrowable(e))
            } else {
              logWarning(s"Processor error policy is continue-on-error, returning original DataFrame")
              currentDf // Fail case: Return original DataFrame
            }
        }
      }
    }
  }

  /**
   * Processor configuration extraction
   *
   * Extract all configuration items under spark.pista.processor.* from the Spark configuration.
   * (excluding enabled and classes)
   *
   * @param sparkSession Spark Session
   * @param conf Configuration reader (optional, defaults to internal creation if not provided)
   * @return Configuration Map
   */
  def extractConfig(sparkSession: SparkSession, conf: ConfigReader = null): Map[String, String] = {
    val effectiveConf = if (conf != null) conf else ConfigReader(sparkSession)
    val excludeKeys = Set("enabled", "classes")

    effectiveConf.getAllWithPrefix(SubmitterConf.PROCESSOR_PREFIX)
      .filterNot { case (key, _) => excludeKeys.contains(key) }
  }
}
