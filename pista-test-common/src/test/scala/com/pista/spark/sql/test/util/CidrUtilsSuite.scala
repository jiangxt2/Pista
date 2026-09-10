package com.pista.spark.sql.test.util

import org.scalatest.funsuite.AnyFunSuite

class CidrUtilsSuite extends AnyFunSuite {
  test("adjacent /24 networks do not overlap") {
    assert(!CidrUtils.overlaps("172.16.1.0/24", "172.16.2.0/24"))
  }

  test("a /16 blocks all candidate /24 networks inside it") {
    val used = Seq("172.16.0.0/16")
    assert(CidrUtils.nextNonOverlappingPrivate24(used, 0).startsWith("172.17."))
  }

  test("candidate order is stable") {
    assert(CidrUtils.nextNonOverlappingPrivate24(Nil, 0) == "172.16.0.0/24")
    assert(CidrUtils.nextNonOverlappingPrivate24(Nil, 1) == "172.16.1.0/24")
  }

  test("occupied candidate is skipped") {
    assert(CidrUtils.nextNonOverlappingPrivate24(Seq("172.16.0.0/24"), 0) == "172.16.1.0/24")
  }

  test("IPv4 CIDR detection rejects Docker IPv6 subnets") {
    assert(CidrUtils.isIpv4Cidr("172.16.0.0/24"))
    assert(!CidrUtils.isIpv4Cidr("fc00:f853:ccd:e793::/64"))
  }
}
