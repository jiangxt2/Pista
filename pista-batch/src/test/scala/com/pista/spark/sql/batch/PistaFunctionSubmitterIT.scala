package com.pista.spark.sql.batch

import com.pista.spark.sql.catalyst.functions.catalog.{FunctionManifest, PistaFunctionCatalog}

import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.io.Source

/** Exercises the packaged Catalog through a real Spark 3.5.8 spark-submit process. */
class PistaFunctionSubmitterIT extends BatchSubmitterIT {
  test("Pista assembly packages the exact generated function manifest") {
    val jar = new JarFile(submitterJarPath.toFile)
    try {
      val entry = Option(jar.getJarEntry("META-INF/pista/functions-v1.json"))
        .getOrElse(fail("Pista assembly is missing META-INF/pista/functions-v1.json"))
      val stream = jar.getInputStream(entry)
      val packaged = try Source.fromInputStream(stream, "UTF-8").mkString
      finally stream.close()
      assert(packaged == FunctionManifest.render(PistaFunctionCatalog.definitions))
    } finally jar.close()
  }

  test("Pista assembly installs scalar, aggregate, and table functions") {
    submit(
      """SELECT pista_ip_family('127.0.0.1'),
        |       pista_json_is_valid('{"a":1}'),
        |       pista_vector_inner_product(array(1D, 2D), array(3D, 4D));
        |SELECT pista_roaring64_cardinality(pista_roaring64_build(id))
        |FROM VALUES (3L), (1L), (3L) AS input(id);
        |SELECT value
        |FROM pista_roaring64_explode(
        |  pista_roaring64_from_array(array(3L, 1L)),
        |  10
        |)""".stripMargin,
      Map("spark.pista.output.format" -> "console"))
  }

  test("Pista assembly preserves compliance metadata and merged service providers") {
    val jar = new JarFile(submitterJarPath.toFile)
    try {
      val names = jar.entries().asScala.map(_.getName).toVector
      assert(names.contains("META-INF/LICENSE"))
      assert(names.contains("META-INF/NOTICE"))
      assert(!names.exists(name =>
        name.startsWith("META-INF/") &&
          (name.endsWith(".SF") || name.endsWith(".RSA") || name.endsWith(".DSA"))))

      assert(serviceProviders(jar,
        "com.pista.spark.sql.execution.datasources.reader.DataReader") == Set(
        "com.pista.spark.sql.connector.clickhouse.ClickHouseReader",
        "com.pista.spark.sql.connector.doris.DorisReader"))
      assert(serviceProviders(jar,
        "com.pista.spark.sql.execution.datasources.writer.DataWriter") == Set(
        "com.pista.spark.sql.connector.clickhouse.ClickHouseWriter",
        "com.pista.spark.sql.connector.doris.DorisWriter"))
    } finally jar.close()
  }

  private def serviceProviders(jar: JarFile, service: String): Set[String] = {
    val entry = Option(jar.getJarEntry(s"META-INF/services/$service"))
      .getOrElse(fail(s"Pista assembly is missing the $service service descriptor"))
    val stream = jar.getInputStream(entry)
    try Source.fromInputStream(stream, "UTF-8").getLines()
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .toSet
    finally stream.close()
  }
}
