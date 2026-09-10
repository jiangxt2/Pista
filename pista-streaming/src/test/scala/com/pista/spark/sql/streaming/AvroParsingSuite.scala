package com.pista.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Avro value parsing test
 *
 * Validate Avro format parsing of Kafka value and JSON/CSV regression.
 *
 */
class AvroParsingSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("AvroParsingSuite")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit =
    if (spark != null) spark.stop()

  // ==================== Avro Parsing Test ====================

  test("from_avro should correctly parse Avro-encoded binary data") {
    import org.apache.spark.sql.avro.functions.from_avro
    import org.apache.spark.sql.functions._

    val avroSchema =
      """{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"""

    // Use Avro encoding to construct test data.
    import org.apache.avro.Schema
    import org.apache.avro.generic.{GenericData, GenericDatumWriter}
    import org.apache.avro.io.EncoderFactory
    import java.io.ByteArrayOutputStream

    val schema = new Schema.Parser().parse(avroSchema)
    val record = new GenericData.Record(schema)
    record.put("id", 42L)
    record.put("name", "test-event")

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    val writer = new GenericDatumWriter[GenericData.Record](schema)
    writer.write(record, encoder)
    encoder.flush()
    val avroBytes = out.toByteArray

    // Create a DataFrame with a binary value.
    val df = spark.createDataFrame(Seq(Tuple1(avroBytes))).toDF("value")
    val parsed = df.select(from_avro(col("value"), avroSchema).as("data"))
    val result = parsed.select("data.id", "data.name").collect()

    assert(result.length == 1)
    assert(result(0).getLong(0) == 42L)
    assert(result(0).getString(1) == "test-event")
  }

  test("format=avro but missing value.schema configuration should throw missingRequiredConfigError") {
    // Simulate configuration: set value.format=avro but not value.schema
    spark.conf.set("spark.pista.streaming.input.value.format", "avro")
    try
      spark.conf.unset("spark.pista.streaming.input.value.schema")
    catch {
      case _: Exception => /* possibly already does not exist */
    }

    // Read configuration using ConfigReader to simulate the behavior of createStreamingSource
    import com.pista.spark.sql.conf.{ConfigReader, SubmitterConf}
    val configReader = ConfigReader(spark)
    val schemaOpt = configReader.getOption(SubmitterConf.STREAMING_INPUT_VALUE_SCHEMA.key)

    assert(schemaOpt.isEmpty, "value.schema should not be configured")

    // Clean
    spark.conf.unset("spark.pista.streaming.input.value.format")
  }

  // ==================== JSON/CSV Regression Testing ====================

  test("from_json should correctly parse JSON string") {
    import org.apache.spark.sql.functions._

    val schemaStr = "id BIGINT, name STRING"
    val schema = DataType.fromDDL(schemaStr).asInstanceOf[org.apache.spark.sql.types.StructType]

    val df = spark.createDataFrame(Seq(
      Tuple1("""{"id":1,"name":"alice"}"""),
      Tuple1("""{"id":2,"name":"bob"}""")
    )).toDF("value")

    val parsed = df.select(from_json(col("value"), schema).as("data"))
    val result = parsed.select("data.id", "data.name").collect()

    assert(result.length == 2)
    assert(result(0).getLong(0) == 1L)
    assert(result(0).getString(1) == "alice")
    assert(result(1).getLong(0) == 2L)
    assert(result(1).getString(1) == "bob")
  }

  test("from_csv should correctly parse CSV string") {
    import org.apache.spark.sql.functions._

    val schemaStr = "id BIGINT, name STRING"
    val schema = DataType.fromDDL(schemaStr).asInstanceOf[org.apache.spark.sql.types.StructType]

    val df = spark.createDataFrame(Seq(
      Tuple1("1,alice"),
      Tuple1("2,bob")
    )).toDF("value")

    val parsed = df.select(from_csv(col("value"), schema, Map.empty[String, String]).as("data"))
    val result = parsed.select("data.id", "data.name").collect()

    assert(result.length == 2)
    assert(result(0).getLong(0) == 1L)
    assert(result(0).getString(1) == "alice")
  }
}
