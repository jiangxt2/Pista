package com.pista.spark.sql.connector.doris.batch

import com.pista.spark.sql.connector.BatchWriteConfig

/** Write mode */
sealed trait WriteMode {
  /** Serializes to lowercase to match the doris_task_info.write_mode contract. */
  def value: String
}
object WriteMode {
  case object SPARK_CONNECTOR extends WriteMode { def value = "spark_connector" }
  case object STREAM_LOAD     extends WriteMode { def value = "stream_load" }
  case object BROKER_LOAD     extends WriteMode { def value = "broker_load" }

  def fromString(s: String): WriteMode = s.toLowerCase match {
    case "spark_connector" => SPARK_CONNECTOR
    case "stream_load"     => STREAM_LOAD
    case "broker_load"     => BROKER_LOAD
    case other             => throw new IllegalArgumentException(s"Unknown writeMode: $other")
  }
}

/**
 * Doris batch write configuration
 *
 * Columnarized with ClickHouseBatchConfig, for use by DorisWriter and each Writer implementation.
 *
 * @param fenodes           FE node addresses, comma-separated (e.g., "fe1:8030,fe2:8030")
 * @param database          Target database name
 * @param table             Target table name
 * @param user              User Authentication Username
 * @param password          Authentication password
 * @param writeMode         Write mode (spark_connector / stream_load)
 * @param labelPrefix       Stream Load Label Prefix used for idempotent labeling
 * @param outputPartitions Write the target number of partitions before performing repartitioning.
 * @param batchSize         Connector internal batch Stream Load Batch Size
 * @param dataFormat        Data format: arrow (recommended) / csv / json
 * @param enable2PC         Does enabling Exactly-Once 2PC transactions
 * @param autoRedirect      true routes through FE (default); false connects directly to BE and requires benodes
 *                          Docker/NAT and other BE internal IP unreachable environments must be set to false
 * @param benodes           List of BE HTTP nodes, comma-separated (e.g. "be1:8040,be2:8040").
 *                          autoRedirect=false required
 * @param sourceTable       source table in catalog format, for example iceberg_db.source_table
 * @param feQueryPort        Doris FE MySQL query port (for JDBC queries and information_schema / ADMIN commands)
 * @param overwrite          Whether overwriting occurs.partitionDate overwriting the specified partition when it is non-empty (temp partition + REPLACE PARTITION
 *                           Replace atomically; overwrite entire table when partitionDate is null (ALTER TABLE REPLACE atomically)
 * @param partitionDate      Date partition value, comma-separated (e.g., "20260407" or "20260407,20260408");
 *                           overwrite=true When used, the partition name p<dateValue> is automatically derived internally.
 * @param partitionColumn    date partition column (for example, "partition_date"); when overwrite=true and
 *                           partitionDate is non-empty, it filters the source before writing
 *                           Is empty to not filter
 */
case class DorisBatchConfig(
  fenodes:             String,
  database:            String,
  table:               String,
  user:                String         = "root",
  password:            String,
  writeMode:           WriteMode      = WriteMode.SPARK_CONNECTOR,
  labelPrefix:         String         = "pista",
  outputPartitions:    Int            = 50,
  batchSize:           Int            = 500000,
  dataFormat:          String         = "csv",
  enable2PC:           Boolean        = false,
  autoRedirect:        Boolean        = true,
  benodes:             String         = "",
  sourceTable:         Option[String] = None,
  feQueryPort:         Int            = 9030,
  overwrite:           Boolean        = false,
  partitionDate:       String         = "",
  partitionColumn:     String         = ""
) extends BatchWriteConfig {
  /** partitionDate is not null for partitioned overwrite, otherwise for table-wide overwrite */
  def isPartitionOverwrite: Boolean = overwrite && partitionDate.nonEmpty

  /** Derive the list of formal partition names from partitionDate, with the Doris Auto Partition naming rule convention: pyyyyMMdd */
  def targetPartitions: Seq[String] =
    if (partitionDate.isEmpty) Nil
    else partitionDate.split(",").map(d => s"p${d.trim}").toSeq

  /**
   * Infer partition filter conditions automatically based on partitionColumn + partitionDate.
   * A single date produces `column = 'date'`; comma-separated dates produce `column IN ('date1', 'date2')`.
   * None when neither column nor date is present (table-wide statistics).
   */
  def resolvePartitionFilter(): Option[String] =
    if (partitionColumn.nonEmpty && partitionDate.nonEmpty) {
      val dates = partitionDate.split(",").map(_.trim).filter(_.nonEmpty)
      if (dates.length == 1)
        Some(s"$partitionColumn = '${dates.head}'")
      else
        Some(s"$partitionColumn IN (${dates.map(d => s"'$d'").mkString(", ")})")
    } else None
}
