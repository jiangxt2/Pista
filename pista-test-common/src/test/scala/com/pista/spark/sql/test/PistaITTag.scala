package com.pista.spark.sql.test

import org.scalatest.Tag

/** Tag used by the Maven profile to separate Docker-backed integration tests. */
object DockerIT extends Tag("com.pista.DockerIT")

/** Tag for Submitter ITs that require an assembled pista-assembly JAR. */
object SubmitterIT extends Tag("com.pista.SubmitterIT")
