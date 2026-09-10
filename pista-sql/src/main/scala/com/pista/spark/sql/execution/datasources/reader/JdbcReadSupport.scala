package com.pista.spark.sql.execution.datasources.reader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types._

import java.sql.{DriverManager, Types}
import java.util.Properties

/**
 * JDBC read support trait
 * Provides common JDBC read functionality for databases
 *
 */
trait JdbcReadSupport extends Logging {
  // Self-type: classes mixing in this trait must extend AbstractDataReader.
  self: AbstractDataReader =>

  /** JDBC Driver Class Name */
  protected def jdbcDriver: String

  /**
   * Build JDBC URL
   * Subclasses must implement this method to provide the URL format specific to a particular database.
   * @param path Path (typically in database.table format)
   * @param options User configuration options
   * @return JDBC URL
   */
  protected def buildJdbcUrl(
    path: String,
    options: Map[String, String],
    connectionParams: Map[String, String] = Map.empty
  ): String

  /** Obtain JDBC authentication information */
  protected def getJdbcAuth(options: Map[String, String]): (String, String)

  /**
   * Extract table name from path
   * Default implementation: assume the path format is "database.table" or "table".
   * Subclasses can override this method to provide custom logic
   * @param path Path
   * @return table_name
   */
  protected def extractTableName(path: String): String =
    if (path.contains(".")) path.split("\\.").last else path

  /**
   * Build JDBC Properties
   * Subclasses can override this method to add custom parameters (user, password, socket_timeout, etc.).
   * @param options User configuration options
   * @return JDBC Properties
   */
  protected def buildJdbcProperties(options: Map[String, String]): Properties = new Properties()

  /**
   * JDBC read helper method
   * @param spark SparkSession
   * @param jdbcUrl JDBC URL
   * @param tableName Table name or SQL query
   * @param properties JDBC Properties
   * @param schema Explicit Schema (optional)
   * @param predicates Predicate Pushdown Conditions
   * @param numPartitions Number of partitions
   * @param partitionColumn Partition Column (optional)
   * @param lowerBound Lower Bound for Partition
   * @param upperBound Partition Upper Bound
   * @return DataFrame
   */
  protected def readViaJdbc(
    spark: SparkSession,
    jdbcUrl: String,
    tableName: String,
    properties: Properties,
    schema: Option[StructType] = None,
    predicates: Seq[String] = Seq.empty,
    numPartitions: Int = 1,
    partitionColumn: Option[String] = None,
    lowerBound: Option[Long] = None,
    upperBound: Option[Long] = None
  ): DataFrame = {
    logInfo(s"[$engineName] Reading via JDBC with $numPartitions partition(s)")

    var reader = spark.read
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, tableName)
      .option(JDBCOptions.JDBC_DRIVER_CLASS, jdbcDriver)

    // Apply Explicit Schema
    schema.foreach(s => reader = reader.schema(s))

    // Apply Properties
    properties.forEach { (key, value) =>
      reader = reader.option(key.toString, value.toString)
    }

    // Apply Predicate Pushdown
    if (predicates.nonEmpty) {
      val combinedPredicate = predicates.mkString(" AND ")
      logInfo(s"[$engineName] Applying predicate pushdown: $combinedPredicate")
      reader = reader.option(JDBCOptions.JDBC_PUSHDOWN_PREDICATE, "true")
    }

    // Apply partition parallelism for reading
    partitionColumn match {
      case Some(col) if numPartitions > 1 =>
        logInfo(s"[$engineName] Parallel read: column=$col, partitions=$numPartitions, " +
          s"bounds=[${lowerBound.getOrElse("auto")}, ${upperBound.getOrElse("auto")}]")
        reader = reader
          .option(JDBCOptions.JDBC_PARTITION_COLUMN, col)
          .option(JDBCOptions.JDBC_NUM_PARTITIONS, numPartitions.toString)
        lowerBound.foreach(lb => reader = reader.option(JDBCOptions.JDBC_LOWER_BOUND, lb.toString))
        upperBound.foreach(ub => reader = reader.option(JDBCOptions.JDBC_UPPER_BOUND, ub.toString))
      case _ =>
        // Read from single shard.
    }

    val df = reader.load()
    logInfo(s"[$engineName] Successfully created DataFrame via JDBC")
    df
  }

  /**
   * Validate SQL identifiers (table names, column names), allowing only legal characters to prevent SQL injection
   * Legitimate format: letters, numbers, underscores, dot (database.table), backticks (`name`)
   */
  protected def requireValidIdentifier(name: String): String = {
    require(
      name.matches("[\\w.`]+"),
      s"[$engineName] invalid SQL identifier, execution rejected: $name"
    )
    name
  }

  /**
   * Silently close the AutoCloseable resource, ignoring exceptions during closure (for finally block)
   */
  private def closeQuietly(resource: AutoCloseable): Unit =
    if (resource != null) try resource.close() catch { case _: Exception => }

  /**
   * default Schema inference implementation
   * Subclasses can use this method directly or override it to provide custom logic
   *
   * @param spark SparkSession
   * @param config Input configuration
   * @return Inferred Schema
   */
  def inferSchema(spark: SparkSession, config: InputConfig): Option[StructType] = {
    val path = config.path.get
    val jdbcUrl = buildJdbcUrl(path, config.options)
    val tableName = extractTableName(path)
    val properties = buildJdbcProperties(config.options)
    inferJdbcSchema(jdbcUrl, tableName, properties)
  }

  /**
   * Infer Schema from JDBC source
   */
  private def inferJdbcSchema(
    jdbcUrl: String,
    tableName: String,
    properties: Properties
  ): Option[StructType] = {
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    try {
      Class.forName(jdbcDriver)
      conn = DriverManager.getConnection(jdbcUrl, properties)
      stmt = conn.createStatement()
      rs = stmt.executeQuery(s"SELECT * FROM ${requireValidIdentifier(tableName)} WHERE 1=0")
      val metadata = rs.getMetaData
      val fields = (1 to metadata.getColumnCount).map { i =>
        val colName = metadata.getColumnName(i)
        val sqlType = metadata.getColumnType(i)
        val nullable = metadata.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls
        StructField(colName, jdbcTypeToSparkType(sqlType), nullable)
      }
      Some(StructType(fields))
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        logWarning(s"[$engineName] Schema inference failed with ${e.getClass.getSimpleName}")
        None
    } finally {
      closeQuietly(rs)
      closeQuietly(stmt)
      closeQuietly(conn)
    }
  }

  /**
   * Estimating the number of rows (for partitioning decision)
   */
  protected def estimateRowCount(
    jdbcUrl: String,
    tableName: String,
    properties: Properties
  ): Option[Long] = {
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    try {
      Class.forName(jdbcDriver)
      conn = DriverManager.getConnection(jdbcUrl, properties)
      stmt = conn.createStatement()
      rs = stmt.executeQuery(s"SELECT COUNT(*) FROM ${requireValidIdentifier(tableName)}")
      if (rs.next()) Some(rs.getLong(1)) else None
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        logWarning(s"[$engineName] Row-count estimate failed with ${e.getClass.getSimpleName}")
        None
    } finally {
      closeQuietly(rs)
      closeQuietly(stmt)
      closeQuietly(conn)
    }
  }

  /**
   * JDBC SQL types convert to Spark types
   */
  private def jdbcTypeToSparkType(sqlType: Int): DataType =
    sqlType match {
      case Types.BIT | Types.BOOLEAN => BooleanType
      case Types.TINYINT => ByteType
      case Types.SMALLINT => ShortType
      case Types.INTEGER => IntegerType
      case Types.BIGINT => LongType
      case Types.FLOAT | Types.REAL => FloatType
      case Types.DOUBLE => DoubleType
      case Types.NUMERIC | Types.DECIMAL => DecimalType(38, 18)
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR |
           Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR => StringType
      case Types.DATE => DateType
      case Types.TIME | Types.TIMESTAMP => TimestampType
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => BinaryType
      case _ => StringType // default rollback
    }
}
