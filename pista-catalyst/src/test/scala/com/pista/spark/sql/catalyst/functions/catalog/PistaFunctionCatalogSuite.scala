package com.pista.spark.sql.catalyst.functions.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import com.pista.spark.sql.catalyst.functions.internal.datetime.DateTimeKernel
import com.pista.spark.sql.catalyst.functions.internal.json.JsonKernel
import com.pista.spark.sql.catalyst.functions.internal.network.NetworkKernel
import com.pista.spark.sql.catalyst.functions.internal.roaring.Roaring64Kernel
import com.pista.spark.sql.catalyst.functions.internal.url.{DomainKernel, UrlKernel}
import com.pista.spark.sql.catalyst.functions.internal.vector.VectorKernel
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.io.Source

class PistaFunctionCatalogSuite extends AnyFunSuite {
  test("catalog is immutable, complete, and deterministic") {
    assert(PistaFunctionCatalog.definitions.size == 30)
    assert(PistaFunctionCatalog.definitions.count(_.kind == FunctionKind.Scalar) == 26)
    assert(PistaFunctionCatalog.definitions.count(_.kind == FunctionKind.Aggregate) == 3)
    assert(PistaFunctionCatalog.definitions.count(_.kind == FunctionKind.Table) == 1)
    assert(PistaFunctionCatalog.definitions.map(_.canonicalName).distinct.size == 30)
    assert(PistaFunctionCatalog.definitions.forall(_.canonicalName.startsWith("pista_")))
    assert(FunctionManifest.digest(PistaFunctionCatalog.definitions) == PistaFunctionCatalog.digest)
    assert(FunctionManifest.render(PistaFunctionCatalog.definitions) ==
      FunctionManifest.render(PistaFunctionCatalog.definitions.reverse))
    assert(PistaFunctionCatalog.digest ==
      "7e080b6f8c6ce4a91053dd933c0aa989ac76dd799f42b77e73d462e292e4c755")
  }

  test("documentation changes affect the manifest but not the registration contract") {
    val definition = PistaFunctionCatalog.definitions.collectFirst {
      case scalar: ScalarFunctionDefinition => scalar
    }.get
    val changed = definition.copy(
      documentation = definition.documentation.copy(summary = "Documentation-only correction"))
    val recomputedContract = FunctionManifest.contractDigest(
      definition.canonicalName,
      definition.category,
      definition.kind,
      definition.signatures,
      None)

    assert(recomputedContract == definition.contractDigest)
    assert(changed.contractDigest == definition.contractDigest)
    assert(changed.expressionInfo.getClassName == definition.expressionInfo.getClassName)
    assert(FunctionManifest.digest(Seq(changed)) != FunctionManifest.digest(Seq(definition)))
  }

  test("legacy audit covers every baseline decision and every new function") {
    val source = Source.fromResource("META-INF/pista/legacy-function-audit-v1.json")
    val root = try new ObjectMapper().readTree(source.mkString)
    finally source.close()
    val entries = root.get("entries")
    val replacements = entries.elements()
    val replacementNames = scala.collection.mutable.Set.empty[String]
    var replaceCount = 0
    var deleteCount = 0
    while (replacements.hasNext) {
      val entry = replacements.next()
      entry.get("decision").asText() match {
        case "replace" =>
          replaceCount += 1
          replacementNames += entry.get("replacement").asText()
        case "delete" => deleteCount += 1
        case other => fail(s"unknown audit decision: $other")
      }
    }
    val catalogNames = PistaFunctionCatalog.definitions.map(_.canonicalName).toSet
    assert(entries.size() == 56)
    assert(replaceCount == 20)
    assert(deleteCount == 36)
    assert(replacementNames.subsetOf(catalogNames))
    assert(root.get("newFunctions").size() == 10)
    assert(root.get("newFunctions").elements().asScala
      .map(_.get("name").asText()).toSet.subsetOf(catalogNames))
  }

  test("network kernel is literal-only and canonical") {
    val ipv4 = NetworkKernel.ipToBinary(utf8("192.168.1.1"))
    assert(ipv4.toSeq == Seq(192.toByte, 168.toByte, 1.toByte, 1.toByte))
    assert(NetworkKernel.binaryToIp(ipv4).toString == "192.168.1.1")
    assert(NetworkKernel.binaryToIp(NetworkKernel.ipToBinary(utf8("2001:0db8:0:0:0:0:0:1"))).toString == "2001:db8::1")
    assert(NetworkKernel.ipv4ToLong(utf8("255.255.255.255")) == 4294967295L)
    assert(NetworkKernel.longToIpv4(167772161L).toString == "10.0.0.1")
    assert(NetworkKernel.ipFamily(utf8("::1")) == 6)
    assert(NetworkKernel.ipIsPrivate(utf8("10.0.0.1")))
    assert(NetworkKernel.ipIsPrivate(utf8("fd00::1")))
    assert(!NetworkKernel.ipIsPrivate(utf8("127.0.0.1")))
    assert(NetworkKernel.ipInCidr(utf8("10.2.3.4"), utf8("10.0.0.0/8")))
    assert(NetworkKernel.tryIpToBinary(utf8("example.com")) == null)
    assertThrows[Exception](NetworkKernel.ipToBinary(utf8("127.1")))
    assertThrows[Exception](NetworkKernel.ipInCidr(utf8("10.0.0.1"), utf8("10.0.0.1/8")))
    assertThrows[Exception](NetworkKernel.binaryToIp(Array[Byte](1, 2)))
    assertThrows[Exception](NetworkKernel.longToIpv4(-1L))
    assertThrows[Exception](NetworkKernel.longToIpv4(4294967296L))
    assertThrows[Exception](NetworkKernel.ipPrefix(utf8("127.0.0.1"), 33))
    assert(NetworkKernel.binaryToIp(NetworkKernel.ipToBinary(utf8("::ffff:192.0.2.1")))
      .toString == "::ffff:c000:201")
  }

  test("datetime kernel uses ordered formats and rejects ambiguous local time") {
    val formats = new GenericArrayData(Array(utf8("uuuu-MM-dd"), utf8("uuuuMMdd")))
    assert(DateTimeKernel.tryParseDate(utf8("2024-02-29"), formats) != null)
    assert(DateTimeKernel.tryParseDate(utf8("not-a-date"), formats) == null)
    val timestampFormats = new GenericArrayData(Array(utf8("uuuu-MM-dd HH:mm:ss")))
    assert(DateTimeKernel.tryParseTimestamp(
      utf8("2024-01-15 10:30:00"), timestampFormats, utf8("UTC")) != null)
    assert(DateTimeKernel.tryParseTimestamp(
      utf8("2024-11-03 01:30:00"), timestampFormats, utf8("America/New_York")) == null)
    assertThrows[Exception](DateTimeKernel.tryParseDate(
      utf8("2024-01-01"), new GenericArrayData(Array.fill(17)(utf8("uuuu-MM-dd")))))
    assertThrows[Exception](DateTimeKernel.tryParseDate(
      utf8("2024-01-01"), new GenericArrayData(Array(utf8("[invalid")))))
    assertThrows[Exception](DateTimeKernel.tryParseTimestamp(
      utf8("2024-01-01 00:00:00"), timestampFormats, utf8("Invalid/Timezone")))
    assertThrows[Exception](DateTimeKernel.tryParseDate(
      utf8("x" * 4097), new GenericArrayData(Array(utf8("uuuu-MM-dd")))))
    assert(DateTimeKernel.tryParseDate(
      utf8("+999999999-12-31"), new GenericArrayData(Array(utf8("uuuu-MM-dd")))) == null)
    assert(DateTimeKernel.tryParseTimestamp(
      utf8("+999999999-12-31 23:59:59"),
      new GenericArrayData(Array(utf8("uuuu-MM-dd HH:mm:ss"))), utf8("UTC")) == null)
  }

  test("roaring envelope round trips, validates checksums, and preserves unsigned ordering") {
    assert(Roaring64Kernel.crc32c(
      "123456789".getBytes(java.nio.charset.StandardCharsets.US_ASCII)) == 0xe3069283L)
    val encoded = Roaring64Kernel.fromArray(new GenericArrayData(Array[Long](5L, 1L, 5L)))
    assert(new String(encoded.take(4), java.nio.charset.StandardCharsets.US_ASCII) == "P64B")
    assert(encoded(4) == 1.toByte)
    assert(encoded.sameElements(
      Roaring64Kernel.fromArray(new GenericArrayData(Array[Long](5L, 1L, 5L)))))
    assert(Roaring64Kernel.cardinality(encoded) == 2L)
    assert(Roaring64Kernel.contains(encoded, 1L))
    assert(Roaring64Kernel.explode(encoded, 10).toLongArray.toSeq == Seq(1L, 5L))
    val other = Roaring64Kernel.fromArray(new GenericArrayData(Array[Long](5L, 9L)))
    assert(Roaring64Kernel.cardinality(Roaring64Kernel.union(encoded, other)) == 3L)
    assert(Roaring64Kernel.cardinality(Roaring64Kernel.intersect(encoded, other)) == 1L)
    assert(Roaring64Kernel.cardinality(Roaring64Kernel.xor(encoded, other)) == 2L)
    assert(Roaring64Kernel.cardinality(Roaring64Kernel.andNot(encoded, other)) == 1L)
    val damaged = encoded.clone()
    damaged(damaged.length - 1) = (damaged.last ^ 1).toByte
    assertThrows[Exception](Roaring64Kernel.cardinality(damaged))
    val unknownVersion = encoded.clone()
    unknownVersion(4) = 2.toByte
    assertThrows[Exception](Roaring64Kernel.cardinality(unknownVersion))
    val unknownCodec = encoded.clone()
    unknownCodec(5) = 2.toByte
    assertThrows[Exception](Roaring64Kernel.cardinality(unknownCodec))
    val unsupportedFlags = encoded.clone()
    unsupportedFlags(7) = 1.toByte
    assertThrows[Exception](Roaring64Kernel.cardinality(unsupportedFlags))
    val invalidPayloadLength = encoded.clone()
    invalidPayloadLength(11) = (invalidPayloadLength(11) + 1).toByte
    assertThrows[Exception](Roaring64Kernel.cardinality(invalidPayloadLength))
    assertThrows[Exception](Roaring64Kernel.explode(encoded, 1))
    assertThrows[Exception](Roaring64Kernel.fromArray(new GenericArrayData(Array[Long](1L, -1L))))
    assertThrows[Exception](Roaring64Kernel.fromArray(
      new GenericArrayData(Array[java.lang.Long](1L, null))))
    assertThrows[Exception](Roaring64Kernel.cardinality(Array.fill[Byte](15)(0)))
  }

  test("JSON Merge Patch, domain, and URL kernels are strict and offline") {
    assert(JsonKernel.isValid(utf8("{\"a\":1}")))
    assert(!JsonKernel.isValid(utf8("{\"a\":")))
    assert(JsonKernel.mergePatch(
      utf8("{\"a\":1,\"b\":{\"c\":2}}"),
      utf8("{\"a\":null,\"b\":{\"d\":3}}"))
      .toString == "{\"b\":{\"c\":2,\"d\":3}}")
    assert(DomainKernel.registrableDomain(utf8("www.example.co.uk")).toString == "example.co.uk")
    assert(DomainKernel.registrableDomain(utf8("co.uk")) == null)
    assert(DomainKernel.registrableDomain(utf8("食狮.com.cn")).toString == "xn--85x722f.com.cn")
    assert(DomainKernel.registrableDomain(utf8("a.b.ck")).toString == "a.b.ck")
    assert(DomainKernel.registrableDomain(utf8("www.ck")).toString == "www.ck")
    assert(DomainKernel.registrableDomain(utf8("127.0.0.1")) == null)
    assert(DomainKernel.registrableDomain(utf8("010.0.0.1")) == null)
    assert(DomainKernel.registrableDomain(utf8("1.2.3.999")) == null)
    assert(DomainKernel.registrableDomain(utf8("123.example")).toString == "123.example")
    assert(UrlKernel.tryDecode(utf8("a+b%2Fc")).toString == "a b/c")
    assert(UrlKernel.tryDecode(utf8("%FF")) == null)
    assert(UrlKernel.tryDecode(utf8("%")) == null)
    assert(!JsonKernel.isValid(utf8("{} {}")))
    assertThrows[Exception](JsonKernel.mergePatch(utf8("{"), utf8("{}")))
    assertThrows[Exception](JsonKernel.mergePatch(utf8(""), utf8("{}")))
    assertThrows[Exception](JsonKernel.mergePatch(utf8("{}"), utf8("   ")))
  }

  test("vector kernel validates dimensions and finite values") {
    val left = doubles(1.0, 2.0, 3.0)
    val right = doubles(4.0, 5.0, 6.0)
    assert(VectorKernel.innerProduct(left, right) == 32.0)
    assert(VectorKernel.l2Distance(left, right) == Math.sqrt(27.0))
    assert(Math.abs(VectorKernel.cosineSimilarity(left, right) - 0.9746318461970762) < 1e-12)
    val normalized = VectorKernel.l2Normalize(doubles(3.0, 4.0)).toDoubleArray
    assert(normalized.toSeq == Seq(0.6, 0.8))
    assertThrows[Exception](VectorKernel.innerProduct(doubles(1.0), doubles(1.0, 2.0)))
    assertThrows[Exception](VectorKernel.l2Normalize(doubles(0.0, 0.0)))
    assertThrows[Exception](VectorKernel.innerProduct(doubles(Double.NaN), doubles(1.0)))
    assertThrows[Exception](VectorKernel.innerProduct(doubles(Double.PositiveInfinity), doubles(1.0)))
    assertThrows[Exception](VectorKernel.l2Normalize(new GenericArrayData(Array.emptyDoubleArray)))
    assertThrows[Exception](VectorKernel.l2Normalize(
      new GenericArrayData(Array[java.lang.Double](1.0, null))))
    assertThrows[Exception](VectorKernel.l2Normalize(
      new GenericArrayData(Array.fill(VectorKernel.MaxDimension + 1)(1.0))))
  }

  private def utf8(value: String): UTF8String = UTF8String.fromString(value)
  private def doubles(values: Double*): GenericArrayData = new GenericArrayData(values.toArray)
}
