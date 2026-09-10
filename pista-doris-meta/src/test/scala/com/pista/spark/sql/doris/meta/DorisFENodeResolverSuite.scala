package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.doris.meta.record.DorisFERecord
import org.scalatest.funsuite.AnyFunSuite

/**
 *DorisFENodeResolver Unit Test*
 *
 * Test content:
 * - Parameters passed in are returned directly without table lookup. fenodes It directly returns without querying.
 * - unspecified parameters read from doris_fe_info table is queried, and the leader node has priority for ordering.leader node priority order
 * - an empty table with connection=None raises IllegalStateException
 * - Query failures degrade to an empty list and ultimately raise IllegalStateException.
 */
class DorisFENodeResolverSuite extends AnyFunSuite {

  // ==================== Column Prioritization Branch ====================

  test("parameter passed as fenodes directly returns without metadata table lookup") {
    val resolver = new DorisFENodeResolver(None)
    val result = resolver.resolve(
      paramFenodes = Some("  127.0.0.2:8030  "),
      clusterName  = "test_cluster"
    )
    assert(result === "127.0.0.2:8030")
  }

  test("multiple fenodes passed in retain original order") {
    val resolver = new DorisFENodeResolver(None)
    val result = resolver.resolve(
      paramFenodes = Some("fe1:8030,fe2:8030,fe3:8030"),
      clusterName  = "test_cluster"
    )
    assert(result === "fe1:8030,fe2:8030,fe3:8030")
  }

  test("parameter passed as an empty string is considered null and triggers a rollback to table lookup") {
    val resolver = new TestableResolver(Seq.empty)
    val ex = intercept[IllegalStateException] {
      resolver.resolve(paramFenodes = Some("   "), clusterName = "empty_cluster")
    }
    assert(ex.getMessage.contains("Cannot resolve Doris FE nodes"))
  }

  // ==================== Metadata Table Query Branch ====================

  test("From table, query: leader node ranks first") {
    val records = Seq(
      DorisFERecord(1, "c1", "follower1", 9030, 8030, isLeader = false, new java.sql.Timestamp(0)),
      DorisFERecord(2, "c1", "leader1",   9030, 8030, isLeader = true,  new java.sql.Timestamp(0)),
      DorisFERecord(3, "c1", "follower2", 9030, 8030, isLeader = false, new java.sql.Timestamp(0))
    )
    val resolver = new TestableResolver(records)
    val result = resolver.resolve(paramFenodes = None, clusterName = "c1")
    assert(result === "leader1:8030,follower1:8030,follower2:8030")
  }

  test("From the table, query when no leader: sort by host alphabetically") {
    val records = Seq(
      DorisFERecord(1, "c1", "zebra",  9030, 8030, isLeader = false, new java.sql.Timestamp(0)),
      DorisFERecord(2, "c1", "alpha",  9030, 8030, isLeader = false, new java.sql.Timestamp(0))
    )
    val resolver = new TestableResolver(records)
    val result = resolver.resolve(paramFenodes = None, clusterName = "c1")
    assert(result === "alpha:8030,zebra:8030")
  }

  // ==================== Exception Branch ====================

  test("No parameters passed and connection = None results in an IllegalStateException") {
    val resolver = new DorisFENodeResolver(None)
    val ex = intercept[IllegalStateException] {
      resolver.resolve(paramFenodes = None, clusterName = "any")
    }
    assert(ex.getMessage.contains("Cannot resolve Doris FE nodes"))
    assert(ex.getMessage.contains("parameter not provided"))
  }

  test("No parameters passed and table is empty when throwing IllegalStateException") {
    val resolver = new TestableResolver(Seq.empty)
    val ex = intercept[IllegalStateException] {
      resolver.resolve(paramFenodes = None, clusterName = "empty_cluster")
    }
    assert(ex.getMessage.contains("Cannot resolve Doris FE nodes"))
    assert(ex.getMessage.contains("no records in doris_fe_info"))
  }

  // ==================== Helper Class ====================

  private def dummyMetaConn: DorisMetaConnection =
    new DorisMetaConnection(
      DorisMetaConf("127.0.0.1", 5432, "test", "test", "test", "test")
    )

  /**
   * Testable Resolver: Fixedly returns preset FE records
   */
  class TestableResolver(records: Seq[DorisFERecord])
    extends DorisFENodeResolver(Some(dummyMetaConn)) {
    override private[meta] def queryFERecords(
      conn:        DorisMetaConnection,
      clusterName: String
    ): Seq[DorisFERecord] = records
  }

}
