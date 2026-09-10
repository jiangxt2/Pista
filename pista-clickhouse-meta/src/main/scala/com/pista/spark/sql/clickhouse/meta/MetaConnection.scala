package com.pista.spark.sql.clickhouse.meta

import org.apache.spark.internal.Logging

import java.sql.{Connection, DriverManager}

/**
 * Metadata database connection management for PostgreSQL.
 *
 * Unlike the legacy singleton, each instance owns an independent connection.
 * - class Instead of object, supports dependency injection and testing.
 * - synchronized ensure thread safety of getConnection in a multithreaded environment
 *
 * @param conf Configuration metadata database configuration
 */
class MetaConnection(conf: MetaConf) extends Logging with AutoCloseable {

  private var _connection: Connection = connect()

  private def connect(): Connection = {
    logInfo("Connecting to the configured PostgreSQL metadata store")
    val conn = DriverManager.getConnection(conf.postgresUrl, conf.username, conf.password)
    logInfo("Connected to PostgreSQL meta store")
    conn
  }

  /** Get valid connection (automatically reconnects on failure) */
  def getConnection: Connection = synchronized {
    if (_connection.isClosed || !_connection.isValid(10))
      _connection = connect()
    _connection
  }

  override def close(): Unit =
    if (!_connection.isClosed) _connection.close()
}
