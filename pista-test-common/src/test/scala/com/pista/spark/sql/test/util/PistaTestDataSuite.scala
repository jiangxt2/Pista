package com.pista.spark.sql.test.util

import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.{Callable, Executors, TimeUnit}

class TestDataGeneratorSuite extends AnyFunSuite {
  test("uniqueName generate a valid and bounded database identifier") {
    val first  = TestDataGenerator.uniqueName("123 Bad.Name")
    val second = TestDataGenerator.uniqueName("123 Bad.Name")

    assert(first.matches("[a-z_][a-z0-9_]*"))
    assert(first.length <= 63)
    assert(first != second)
  }

  test("uniqueBucketName generate a valid and length-constrained S3 bucket name S3 bucket name") {
    val bucket = TestDataGenerator.uniqueBucketName(
      "Pista_IT.Bucket_" + ("long-prefix-" * 10))

    assert(bucket.length >= 3)
    assert(bucket.length <= 63)
    assert(bucket.matches("[a-z0-9][a-z0-9-]*[a-z0-9]"))
  }

  test("Unique names generated under concurrent loads remain unique") {
    val pool = Executors.newFixedThreadPool(8)
    val futures = (1 to 256).map { _ =>
      pool.submit(new Callable[String] {
        override def call(): String = TestDataGenerator.uniqueName("concurrent_it")
      })
    }

    val names = try {
      futures.map(_.get(10, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }

    assert(names.distinct.size == names.size)
  }
}
