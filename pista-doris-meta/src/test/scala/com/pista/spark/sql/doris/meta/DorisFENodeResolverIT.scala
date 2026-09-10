package com.pista.spark.sql.doris.meta

import com.pista.spark.sql.test.base.BaseIT
import com.pista.spark.sql.test.container.ContainerSuite
import com.pista.spark.sql.test.util.MetaSchemaInitializer

/** Resolves a real FE record initialized from the Dockerized Doris instance. */
class DorisFENodeResolverIT extends BaseIT {

  test("DorisFENodeResolver can parse addresses from real FE metadata") {
    val postgres = ContainerSuite.postgres
    val doris = ContainerSuite.doris
    MetaSchemaInitializer.initializeDoris(
      postgres.metaJdbcUrl, postgres.metaUser, postgres.metaPassword, doris)
    val metaConf = DorisMetaConf(
      pgHost = postgres.getHost,
      pgPort = postgres.getMappedPort(5432),
      pgDatabase = "pista_meta",
      pgUsername = postgres.metaUser,
      pgPassword = postgres.metaPassword,
      clusterName = "pista_it")
    val connection = new DorisMetaConnection(metaConf)
    try {
      val resolved = new DorisFENodeResolver(Some(connection)).resolve(None, "pista_it")
      assert(resolved == s"${doris.feHost}:${doris.feHttpPort}")
    } finally connection.close()
  }
}
