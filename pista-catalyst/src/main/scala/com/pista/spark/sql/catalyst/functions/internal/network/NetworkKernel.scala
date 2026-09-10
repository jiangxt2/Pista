package com.pista.spark.sql.catalyst.functions.internal.network

import com.pista.spark.errors.PistaUDFErrors
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

object NetworkKernel {
  private val MaxLiteralLength = 64

  def ipToBinary(input: UTF8String): Array[Byte] =
    parseStrict("pista_ip_to_binary", input)

  def tryIpToBinary(input: UTF8String): Array[Byte] =
    parse(input.toString).orNull

  def binaryToIp(input: Array[Byte]): UTF8String = {
    val rendered = input.length match {
      case 4 => renderIpv4(input)
      case 16 => renderIpv6(input)
      case length =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_binary_to_ip",
          s"expected a 4-byte IPv4 or 16-byte IPv6 value, got $length bytes")
    }
    UTF8String.fromString(rendered)
  }

  def ipv4ToLong(input: UTF8String): java.lang.Long = {
    val bytes = parseStrict("pista_ipv4_to_long", input)
    if (bytes.length != 4)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ipv4_to_long", "expected an IPv4 literal")
    java.lang.Long.valueOf(
      ((bytes(0) & 0xffL) << 24) |
        ((bytes(1) & 0xffL) << 16) |
        ((bytes(2) & 0xffL) << 8) |
        (bytes(3) & 0xffL))
  }

  def longToIpv4(value: Long): UTF8String = {
    if (value < 0L || value > 0xffffffffL)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_long_to_ipv4", s"value must be in [0, 4294967295], got $value")
    val bytes = Array(
      ((value >>> 24) & 0xff).toByte,
      ((value >>> 16) & 0xff).toByte,
      ((value >>> 8) & 0xff).toByte,
      (value & 0xff).toByte)
    UTF8String.fromString(renderIpv4(bytes))
  }

  def ipFamily(input: UTF8String): java.lang.Integer =
    java.lang.Integer.valueOf(parseStrict("pista_ip_family", input).length match {
      case 4 => 4
      case 16 => 6
    })

  def ipIsPrivate(input: UTF8String): java.lang.Boolean = {
    val bytes = parseStrict("pista_ip_is_private", input)
    val result = if (bytes.length == 4) {
      val first = bytes(0) & 0xff
      val second = bytes(1) & 0xff
      first == 10 || (first == 172 && second >= 16 && second <= 31) ||
        (first == 192 && second == 168)
    } else {
      (bytes(0) & 0xfe) == 0xfc
    }
    java.lang.Boolean.valueOf(result)
  }

  def ipInCidr(ip: UTF8String, cidr: UTF8String): java.lang.Boolean = {
    val address = parseStrict("pista_ip_in_cidr", ip)
    val (network, prefixLength) = parseCidr(cidr.toString)
    java.lang.Boolean.valueOf(
      address.length == network.length && prefixEquals(address, network, prefixLength))
  }

  def ipPrefix(ip: UTF8String, prefixLength: Int): Array[Byte] = {
    val address = parseStrict("pista_ip_prefix", ip)
    val maxPrefix = address.length * 8
    if (prefixLength < 0 || prefixLength > maxPrefix)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ip_prefix", s"prefix length must be in [0, $maxPrefix], got $prefixLength")
    mask(address, prefixLength)
  }

  private def parseStrict(functionName: String, input: UTF8String): Array[Byte] =
    parse(input.toString).getOrElse(
      throw PistaUDFErrors.functionInvalidInputError(functionName, "expected an IP address literal"))

  private def parse(input: String): Option[Array[Byte]] = {
    if (input.isEmpty || input.length > MaxLiteralLength || input.exists(_.isWhitespace)) None
    else if (input.indexOf(':') >= 0) parseIpv6(input)
    else parseIpv4(input)
  }

  private def parseIpv4(input: String): Option[Array[Byte]] = {
    val parts = input.split("\\.", -1)
    if (parts.length != 4) return None
    val result = new Array[Byte](4)
    var index = 0
    while (index < 4) {
      val part = parts(index)
      if (part.isEmpty || part.length > 3 || (part.length > 1 && part.charAt(0) == '0') ||
          !part.forall(character => character >= '0' && character <= '9')) return None
      val value = try part.toInt catch { case _: NumberFormatException => return None }
      if (value > 255) return None
      result(index) = value.toByte
      index += 1
    }
    Some(result)
  }

  private def parseIpv6(input: String): Option[Array[Byte]] = {
    if (input.indexOf('%') >= 0 || input.startsWith("[") || input.endsWith("]")) return None
    val compression = input.indexOf("::")
    if (compression >= 0 && input.indexOf("::", compression + 2) >= 0) return None

    val (leftText, rightText, compressed) = if (compression >= 0) {
      (input.substring(0, compression), input.substring(compression + 2), true)
    } else (input, "", false)

    val left = parseIpv6Side(leftText, allowIpv4Tail = !compressed || rightText.isEmpty)
    val right = parseIpv6Side(rightText, allowIpv4Tail = true)
    if (left.isEmpty || right.isEmpty) return None
    val leftWords = left.get
    val rightWords = right.get
    val missing = 8 - leftWords.length - rightWords.length
    if ((!compressed && missing != 0) || (compressed && missing < 1)) return None
    val words = leftWords ++ Array.fill(missing)(0) ++ rightWords
    if (words.length != 8) return None
    val result = new Array[Byte](16)
    var index = 0
    while (index < words.length) {
      result(index * 2) = ((words(index) >>> 8) & 0xff).toByte
      result(index * 2 + 1) = (words(index) & 0xff).toByte
      index += 1
    }
    Some(result)
  }

  private def parseIpv6Side(text: String, allowIpv4Tail: Boolean): Option[Array[Int]] = {
    if (text.isEmpty) return Some(Array.emptyIntArray)
    val parts = text.split(":", -1)
    if (parts.exists(_.isEmpty)) return None
    val words = scala.collection.mutable.ArrayBuffer.empty[Int]
    var index = 0
    while (index < parts.length) {
      val part = parts(index)
      if (part.indexOf('.') >= 0) {
        if (!allowIpv4Tail || index != parts.length - 1) return None
        parseIpv4(part) match {
          case Some(bytes) =>
            words += (((bytes(0) & 0xff) << 8) | (bytes(1) & 0xff))
            words += (((bytes(2) & 0xff) << 8) | (bytes(3) & 0xff))
          case None => return None
        }
      } else {
        if (part.length > 4 || !part.forall(isHexDigit)) return None
        words += Integer.parseInt(part, 16)
      }
      index += 1
    }
    Some(words.toArray)
  }

  private def isHexDigit(character: Char): Boolean =
    (character >= '0' && character <= '9') ||
      (character >= 'a' && character <= 'f') ||
      (character >= 'A' && character <= 'F')

  private def renderIpv4(bytes: Array[Byte]): String =
    bytes.iterator.map(_ & 0xff).mkString(".")

  private def renderIpv6(bytes: Array[Byte]): String = {
    val words = Array.tabulate(8)(index =>
      ((bytes(index * 2) & 0xff) << 8) | (bytes(index * 2 + 1) & 0xff))
    var bestStart = -1
    var bestLength = 0
    var index = 0
    while (index < words.length) {
      if (words(index) == 0) {
        val start = index
        while (index < words.length && words(index) == 0) index += 1
        val length = index - start
        if (length >= 2 && length > bestLength) {
          bestStart = start
          bestLength = length
        }
      } else index += 1
    }

    val builder = new StringBuilder
    index = 0
    while (index < words.length) {
      if (index == bestStart) {
        builder.append("::")
        index += bestLength
      } else {
        if (builder.nonEmpty && builder.last != ':') builder.append(':')
        builder.append(Integer.toHexString(words(index)).toLowerCase(Locale.ROOT))
        index += 1
      }
    }
    if (builder.isEmpty) "::" else builder.toString()
  }

  private def parseCidr(input: String): (Array[Byte], Int) = {
    val separator = input.indexOf('/')
    if (separator <= 0 || separator != input.lastIndexOf('/') || separator == input.length - 1)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ip_in_cidr", "expected CIDR in address/prefix form")
    val network = parse(input.substring(0, separator)).getOrElse(
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ip_in_cidr", "CIDR contains an invalid address literal"))
    val prefix = try input.substring(separator + 1).toInt catch {
      case _: NumberFormatException =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_ip_in_cidr", "CIDR prefix is not an integer")
    }
    val maxPrefix = network.length * 8
    if (prefix < 0 || prefix > maxPrefix)
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ip_in_cidr", s"CIDR prefix must be in [0, $maxPrefix], got $prefix")
    if (!java.util.Arrays.equals(network, mask(network, prefix)))
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_ip_in_cidr", "CIDR address contains non-zero host bits")
    (network, prefix)
  }

  private def mask(address: Array[Byte], prefixLength: Int): Array[Byte] = {
    val result = address.clone()
    var bit = prefixLength
    var index = 0
    while (index < result.length) {
      if (bit >= 8) bit -= 8
      else if (bit <= 0) result(index) = 0
      else {
        result(index) = (result(index) & (0xff << (8 - bit))).toByte
        bit = 0
      }
      index += 1
    }
    result
  }

  private def prefixEquals(left: Array[Byte], right: Array[Byte], prefixLength: Int): Boolean =
    java.util.Arrays.equals(mask(left, prefixLength), right)
}
