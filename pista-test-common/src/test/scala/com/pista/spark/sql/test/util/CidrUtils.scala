package com.pista.spark.sql.test.util

import scala.util.Try

/** Small IPv4 CIDR helper used only for dynamically isolated Docker networks. */
object CidrUtils {
  private val FirstPrivateOctet = 172
  private val FirstPrivateSecondOctet = 16
  private val LastPrivateSecondOctet = 31
  private val CandidateCount =
    (LastPrivateSecondOctet - FirstPrivateSecondOctet + 1) * 256

  def nextNonOverlappingPrivate24(used: Seq[String], attempt: Int): String = {
    require(attempt >= 0, s"attempt must be non-negative: $attempt")
    val start = attempt % CandidateCount
    (0 until CandidateCount)
      .map(i => candidate((start + i) % CandidateCount))
      .find(subnet => !used.exists(overlaps(subnet, _)))
      .getOrElse(throw new IllegalStateException(
        "No available /24 subnet in RFC1918 172.16.0.0/12 range"))
  }

  def overlaps(left: String, right: String): Boolean = {
    val (leftStart, leftEnd) = range(left)
    val (rightStart, rightEnd) = range(right)
    leftStart <= rightEnd && rightStart <= leftEnd
  }

  def isIpv4Cidr(cidr: String): Boolean = Try(range(cidr)).isSuccess

  private def candidate(index: Int): String = {
    val second = FirstPrivateSecondOctet + index / 256
    val third = index % 256
    s"$FirstPrivateOctet.$second.$third.0/24"
  }

  private def range(cidr: String): (Long, Long) = {
    val parts = cidr.split("/", -1)
    require(parts.length == 2, s"Invalid CIDR: $cidr")
    val prefix = parts(1).toInt
    require(prefix >= 0 && prefix <= 32, s"Invalid CIDR prefix: $cidr")
    val address = ipv4(parts(0))
    val mask = if (prefix == 0) 0L else (0xffffffffL << (32 - prefix)) & 0xffffffffL
    val start = address & mask
    (start, start | (0xffffffffL ^ mask))
  }

  private def ipv4(value: String): Long = {
    val octets = value.split("\\.", -1)
    require(octets.length == 4, s"Invalid IPv4 address: $value")
    octets.foldLeft(0L) { (result, octet) =>
      val n = octet.toInt
      require(n >= 0 && n <= 255, s"Invalid IPv4 address: $value")
      (result << 8) | n
    }
  }
}
