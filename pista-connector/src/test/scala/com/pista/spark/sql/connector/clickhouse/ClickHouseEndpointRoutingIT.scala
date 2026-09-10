package com.pista.spark.sql.connector.clickhouse

import com.pista.spark.sql.test.base.ClickHouseBaseIT

class ClickHouseEndpointRoutingIT extends ClickHouseBaseIT {
  test("ClickHouse Meta machine records use endpoint for each node. host:port endpoint") {
    assert(machineRecords.map(_.hostPort).toSet == clickHouseNodes.toSet)
    (0 until ckContainer.nodeCount).foreach { index =>
      assert(ckContainer.queryLong(index, "SELECT 1") == 1L)
    }
  }
}
