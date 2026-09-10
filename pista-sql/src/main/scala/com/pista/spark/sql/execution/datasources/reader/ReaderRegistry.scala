package com.pista.spark.sql.execution.datasources.reader

import com.pista.spark.errors.PistaErrors
import com.pista.spark.util.LogRedaction
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.ServiceLoader
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Reader registry with automatic discovery via ServiceLoader
 *
 * Reference Spark DataSource implementation, use ServiceLoader to auto-discover and register Reader
 *
 */
object ReaderRegistry extends Logging {

  // Use ServiceLoader to automatically discover all Readers (lazy initialization)
  private lazy val readers: Map[String, DataReader] = {
    logInfo("Discovering Readers via ServiceLoader...")

    val loader = ServiceLoader.load(classOf[DataReader])
    val discovered = loader.iterator().asScala.toSeq

    val readerMap = discovered.map { reader =>
      logInfo(s"Registered Reader: ${reader.name} (${reader.getClass.getName})")
      reader.name -> reader
    }.toMap

    if (readerMap.isEmpty)
      logWarning("No Readers found via ServiceLoader. Check META-INF/services configuration.")
    else
      logInfo(s"Total ${readerMap.size} Reader(s) available: ${readerMap.keys.mkString(", ")}")

    readerMap
  }

  /**
   * Read DataFrame using appropriate reader
   */
  def read(spark: SparkSession, config: InputConfig): DataFrame =
    readers.get(config.format.toLowerCase) match {
      case Some(reader) =>
        logInfo(s"Reading with ${reader.name}")
        Try(reader.read(spark, config)) match {
          case Success(df) =>
            logInfo(s"Successfully read data using ${reader.name}")
            df
          case Failure(e) =>
            logError(
              s"Failed to read data using ${reader.name} with ${LogRedaction.exceptionName(e)}",
              LogRedaction.sanitizedThrowable(e))
            throw LogRedaction.sanitizedThrowable(e)
        }

      case None =>
        throw PistaErrors.readerNotFoundError(config.format, availableFormats)
    }

  /**
   * Get reader by name (for direct access)
   */
  def getReader(name: String): Option[DataReader] = readers.get(name.toLowerCase)

  /**
   * List all available reader names
   */
  def availableFormats: Seq[String] = readers.keys.toSeq.sorted

  /**
   * Check if a format is supported
   */
  def isSupported(format: String): Boolean = readers.contains(format.toLowerCase)
}
