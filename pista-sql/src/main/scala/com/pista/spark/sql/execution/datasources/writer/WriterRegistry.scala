package com.pista.spark.sql.execution.datasources.writer

import com.pista.spark.errors.PistaErrors
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

import java.util.ServiceLoader
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Writer registry with automatic discovery via ServiceLoader
 *
 * Uses ServiceLoader discovery following Spark DataSource extension conventions.
 *
 */
object WriterRegistry extends Logging {

  // Use ServiceLoader to automatically discover all Writers (lazy initialization)
  private lazy val writers: Map[String, DataWriter] = {
    logInfo("Discovering Writers via ServiceLoader...")

    val loader = ServiceLoader.load(classOf[DataWriter])
    val discovered = loader.iterator().asScala.toSeq

    val writerMap = discovered.map { writer =>
      logInfo(s"Registered Writer: ${writer.name} (${writer.getClass.getName})")
      writer.name -> writer
    }.toMap

    if (writerMap.isEmpty)
      logWarning("No Writers found via ServiceLoader. Check META-INF/services configuration.")
    else
      logInfo(s"Total ${writerMap.size} Writer(s) available: ${writerMap.keys.mkString(", ")}")

    writerMap
  }

  /**
   * Write DataFrame using appropriate writer
   */
  def write(df: DataFrame, config: OutputConfig): Unit =
    writers.get(config.format.toLowerCase) match {
      case Some(writer) =>
        logInfo(s"Writing with ${writer.name}")
        Try(writer.write(df, config)) match {
          case Success(_) =>
            logInfo(s"Successfully wrote data using ${writer.name}")
          case Failure(e) =>
            logError(
              s"Failed to write data using ${writer.name} with ${LogRedaction.exceptionName(e)}",
              LogRedaction.sanitizedThrowable(e))
            throw LogRedaction.sanitizedThrowable(e)
        }

      case None =>
        throw PistaErrors.writerNotFoundError(config.format, availableFormats)
    }

  /**
   * List all available writer formats
   */
  def availableFormats: Seq[String] = writers.keys.toSeq.sorted

  /**
   * Check if a format is supported
   */
  def isSupported(format: String): Boolean = writers.contains(format.toLowerCase)
}
