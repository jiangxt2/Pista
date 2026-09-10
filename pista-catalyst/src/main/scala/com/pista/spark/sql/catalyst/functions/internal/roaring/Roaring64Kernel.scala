package com.pista.spark.sql.catalyst.functions.internal.roaring

import com.pista.spark.errors.PistaUDFErrors
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

object Roaring64Kernel {
  private val Magic = Array[Byte]('P', '6', '4', 'B')
  private val FormatVersion: Byte = 1
  private val CodecPortable: Byte = 1
  private val HeaderLength = 16
  private val MaxBinaryBytes = 64 * 1024 * 1024
  private val MaxArrayElements = 1000000
  private val Crc32cPolynomial = 0x82F63B78L.toInt
  private val Crc32cTable: Array[Int] = Array.tabulate(256) { value =>
    var crc = value
    var bit = 0
    while (bit < 8) {
      crc = if ((crc & 1) != 0) (crc >>> 1) ^ Crc32cPolynomial else crc >>> 1
      bit += 1
    }
    crc
  }

  def empty(): Roaring64NavigableMap = new Roaring64NavigableMap()

  def fromArray(values: ArrayData): Array[Byte] = {
    if (values.numElements() > MaxArrayElements)
      throw PistaUDFErrors.functionResourceLimitError(
        "pista_roaring64_from_array", "array elements", MaxArrayElements, values.numElements())
    val bitmap = empty()
    var index = 0
    while (index < values.numElements()) {
      if (values.isNullAt(index))
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_roaring64_from_array", "array cannot contain null")
      addNonNegative(bitmap, values.getLong(index), "pista_roaring64_from_array")
      index += 1
    }
    encode(bitmap)
  }

  def union(left: Array[Byte], right: Array[Byte]): Array[Byte] =
    binary("pista_roaring64_union", left, right, _.or(_))

  def intersect(left: Array[Byte], right: Array[Byte]): Array[Byte] =
    binary("pista_roaring64_intersect", left, right, _.and(_))

  def xor(left: Array[Byte], right: Array[Byte]): Array[Byte] =
    binary("pista_roaring64_xor", left, right, _.xor(_))

  def andNot(left: Array[Byte], right: Array[Byte]): Array[Byte] =
    binary("pista_roaring64_and_not", left, right, _.andNot(_))

  def cardinality(encoded: Array[Byte]): java.lang.Long =
    java.lang.Long.valueOf(decode("pista_roaring64_cardinality", encoded).getLongCardinality)

  def contains(encoded: Array[Byte], value: Long): java.lang.Boolean = {
    requireNonNegative(value, "pista_roaring64_contains")
    java.lang.Boolean.valueOf(decode("pista_roaring64_contains", encoded).contains(value))
  }

  def explode(encoded: Array[Byte], maxRows: Int): ArrayData = {
    if (maxRows <= 0 || maxRows > MaxArrayElements)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_roaring64_explode", s"max_rows must be in [1, $MaxArrayElements]")
    val bitmap = decode("pista_roaring64_explode", encoded)
    val cardinality = bitmap.getLongCardinality
    if (cardinality > maxRows)
      throw PistaUDFErrors.functionResourceLimitError(
        "pista_roaring64_explode", "output rows", maxRows, cardinality)
    val values = new Array[Long](cardinality.toInt)
    val iterator = bitmap.getLongIterator
    var index = 0
    while (iterator.hasNext) {
      values(index) = iterator.next()
      index += 1
    }
    new GenericArrayData(values)
  }

  def addNonNegative(
      bitmap: Roaring64NavigableMap,
      value: Long,
      functionName: String): Roaring64NavigableMap = {
    requireNonNegative(value, functionName)
    bitmap.addLong(value)
    bitmap
  }

  def unionEncoded(
      bitmap: Roaring64NavigableMap,
      encoded: Array[Byte],
      functionName: String): Roaring64NavigableMap = {
    bitmap.or(decode(functionName, encoded))
    bitmap
  }

  def encode(bitmap: Roaring64NavigableMap): Array[Byte] = {
    bitmap.runOptimize()
    val payloadStream = new ByteArrayOutputStream()
    val payloadOutput = new DataOutputStream(payloadStream)
    bitmap.serializePortable(payloadOutput)
    payloadOutput.close()
    val payload = payloadStream.toByteArray
    if (payload.length > MaxBinaryBytes)
      throw PistaUDFErrors.functionResourceLimitError(
        "roaring64", "serialized bytes", MaxBinaryBytes, payload.length)
    val envelope = ByteBuffer.allocate(HeaderLength + payload.length)
    envelope.put(Magic)
    envelope.put(FormatVersion)
    envelope.put(CodecPortable)
    envelope.putShort(0.toShort)
    envelope.putInt(payload.length)
    envelope.putInt(crc32c(payload).toInt)
    envelope.put(payload)
    envelope.array()
  }

  def decode(functionName: String, encoded: Array[Byte]): Roaring64NavigableMap = {
    if (encoded.length < HeaderLength || encoded.length > HeaderLength + MaxBinaryBytes)
      invalidBinary(functionName, "invalid envelope length")
    val buffer = ByteBuffer.wrap(encoded)
    val magic = new Array[Byte](Magic.length)
    buffer.get(magic)
    if (!java.util.Arrays.equals(magic, Magic)) invalidBinary(functionName, "invalid magic")
    if (buffer.get() != FormatVersion) invalidBinary(functionName, "unsupported format version")
    if (buffer.get() != CodecPortable) invalidBinary(functionName, "unsupported bitmap codec")
    if (buffer.getShort() != 0) invalidBinary(functionName, "unsupported envelope flags")
    val payloadLength = buffer.getInt()
    val expectedChecksum = buffer.getInt()
    if (payloadLength < 0 || payloadLength != encoded.length - HeaderLength)
      invalidBinary(functionName, "payload length does not match envelope")
    val payload = new Array[Byte](payloadLength)
    buffer.get(payload)
    if (crc32c(payload).toInt != expectedChecksum)
      invalidBinary(functionName, "checksum mismatch")
    val bitmap = empty()
    val input = new DataInputStream(new ByteArrayInputStream(payload))
    try bitmap.deserializePortable(input) catch {
      case exception: Exception => invalidBinary(functionName, exception.getMessage)
    } finally input.close()
    bitmap
  }

  private def binary(
      functionName: String,
      left: Array[Byte],
      right: Array[Byte],
      operation: (Roaring64NavigableMap, Roaring64NavigableMap) => Unit): Array[Byte] = {
    val result = decode(functionName, left)
    operation(result, decode(functionName, right))
    encode(result)
  }

  private def requireNonNegative(value: Long, functionName: String): Unit =
    if (value < 0)
      throw PistaUDFErrors.functionInvalidInputError(functionName, s"value must be non-negative: $value")

  /** Castagnoli CRC32C implemented without the Java 9-only java.util.zip.CRC32C class. */
  private[catalyst] def crc32c(bytes: Array[Byte]): Long = {
    var crc = 0xffffffff
    var index = 0
    while (index < bytes.length) {
      crc = Crc32cTable((crc ^ (bytes(index) & 0xff)) & 0xff) ^ (crc >>> 8)
      index += 1
    }
    (~crc).toLong & 0xffffffffL
  }

  private def invalidBinary(functionName: String, reason: String): Nothing =
    throw PistaUDFErrors.functionBinaryFormatError(functionName, Option(reason).getOrElse("invalid payload"))
}
