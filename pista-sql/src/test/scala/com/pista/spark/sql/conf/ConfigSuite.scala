package com.pista.spark.sql.conf

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 */
class ConfigSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("ConfigSuite")
      .config("spark.pista.test.string", "hello")
      .config("spark.pista.test.int", "42")
      .config("spark.pista.test.boolean", "true")
      .config("spark.pista.test.seq", "a,b,c")
      .config("spark.pista.output.options.key1", "value1")
      .config("spark.pista.output.options.key2", "value2")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ==================== ConfigBuilder Test ====================

  test("ConfigBuilder creates a string entry") {
    val entry = ConfigBuilder("test.key")
      .doc("test configuration")
      .version("1.0.0")
      .stringConf
      .createWithDefault("default")

    assert(entry.key == "test.key")
    assert(entry.doc == "test configuration")
    assert(entry.version == "1.0.0")
    assert(entry._defaultValue == "default")
    assert(entry.parse("value") == "value")
  }

  test("ConfigBuilder creates an integer entry") {
    val entry = ConfigBuilder("test.int")
      .doc("integer configuration")
      .intConf
      .createWithDefault(10)

    assert(entry._defaultValue == 10)
    assert(entry.parse("42") == 42)
    assert(entry.stringify(100) == "100")
  }

  test("ConfigBuilder should correctly create a boolean configuration item") {
    val entry = ConfigBuilder("test.bool")
      .booleanConf
      .createWithDefault(false)

    assert(entry._defaultValue == false)
    assert(entry.parse("true") == true)
    assert(entry.parse("false") == false)
  }

  test("ConfigBuilder the ConfigBuilder should correctly create a sequence of configuration items") {
    val entry = ConfigBuilder("test.seq")
      .stringSeqConf
      .createWithDefault(Seq.empty)

    assert(entry.parse("a,b,c") == Seq("a", "b", "c"))
    assert(entry.parse("a, b, c") == Seq("a", "b", "c"))
    assert(entry.parse("") == Seq.empty)
    assert(entry.stringify(Seq("x", "y")) == "x,y")
  }

  test("ConfigBuilder the ConfigBuilder should correctly create optional configuration items") {
    val entry = ConfigBuilder("test.optional")
      .stringConf
      .createOptional

    assert(entry.defaultValue.isEmpty)
  }

  // ==================== ConfigReader Testing ====================

  test("ConfigReader the configuration with default values should be read correctly") {
    val conf = ConfigReader(spark)

    val existingEntry = PistaConfigBuilder("test.string")
      .stringConf
      .createWithDefault("default")

    val missingEntry = PistaConfigBuilder("test.missing")
      .stringConf
      .createWithDefault("default")

    assert(conf.get(existingEntry) == "hello")
    assert(conf.get(missingEntry) == "default")
  }

  test("ConfigReader reads optional settings") {
    val conf = ConfigReader(spark)

    val existingEntry = PistaConfigBuilder("test.string")
      .stringConf
      .createOptional

    val missingEntry = PistaConfigBuilder("test.missing")
      .stringConf
      .createOptional

    assert(conf.get(existingEntry) == Some("hello"))
    assert(conf.get(missingEntry).isEmpty)
  }

  test("ConfigReader.require should throw an exception when the configuration is missing") {
    val conf = ConfigReader(spark)

    val missingEntry = PistaConfigBuilder("test.missing")
      .stringConf
      .createOptional

    val ex = intercept[com.pista.spark.errors.PistaConfigException] {
      conf.require(missingEntry)
    }
    assert(ex.getErrorClass == "PISTA_CONFIG_MISSING_REQUIRED")
    assert(ex.getMessage.contains("spark.pista.test.missing"))
  }

  test("ConfigReader should correctly read all configurations with a specified prefix") {
    val conf = ConfigReader(spark)

    val options = conf.getAllWithPrefix("spark.pista.output.options.")

    assert(options.size == 2)
    assert(options("key1") == "value1")
    assert(options("key2") == "value2")
  }

  test("ConfigReader.contains should correctly check for the existence of a configuration") {
    val conf = ConfigReader(spark)

    val existingEntry = PistaConfigBuilder("test.string")
      .stringConf
      .createOptional

    val missingEntry = PistaConfigBuilder("test.missing")
      .stringConf
      .createOptional

    assert(conf.contains(existingEntry))
    assert(!conf.contains(missingEntry))
  }

  // ==================== SubmitterConf Test ====================

  test("SubmitterConf should include all predefined configuration items") {
    assert(SubmitterConf.allEntries.nonEmpty)
    assert(SubmitterConf.allEntries.contains(SubmitterConf.SQL_FILE))
    assert(SubmitterConf.allEntries.contains(SubmitterConf.OUTPUT_FORMAT))
    assert(SubmitterConf.allEntries.contains(SubmitterConf.PROCESSOR_ENABLED))
  }

  test("SubmitterConf The default configuration item should have the correct default value.") {
    assert(SubmitterConf.LOG_LEVEL._defaultValue == "INFO")
    assert(SubmitterConf.OUTPUT_FORMAT._defaultValue == "console")
    assert(SubmitterConf.OUTPUT_MODE._defaultValue == "overwrite")
    assert(SubmitterConf.PROCESSOR_ENABLED._defaultValue == false)
  }

  test("SubmitterConf.generateDoc should generate a Markdown document") {
    val doc = SubmitterConf.generateDoc()

    assert(doc.contains("# Spark SQL Submitter Configuration Reference"))
    assert(doc.contains("spark.pista.sqlFile_"))
    assert(doc.contains("spark.pista.output.format"))
  }

  test("SubmitterConf should expose canonical keys without aliases") {
    assert(SubmitterConf.allEntries.size == 88)
    assert(SubmitterConf.allEntries.forall(_.key.startsWith("spark.pista.")))
    assert(SubmitterConf.allEntries.forall(_.alternatives.isEmpty))
  }
}
