package com.pista.spark.sql.doris.meta

import org.apache.spark.internal.Logging

import java.sql.{Connection, DriverManager}

/**
 * Doris metadata PostgreSQL connection management
 *
 * @param config Metadata configuration
 *
 */
class DorisMetaConnection(config: DorisMetaConf) extends Logging with AutoCloseable {

  private val pgUrl = s"jdbc:postgresql://${config.pgHost}:${config.pgPort}/${config.pgDatabase}?" +
    s"currentSchema=public&options=--client_encoding=UTF8"

  private var _connection: Connection = _
  private var _initialized = false

  private def connect(): Connection = {
    logInfo("[DorisMetaConnection] Connecting to the configured PostgreSQL metadata store")
    val conn = DriverManager.getConnection(pgUrl, config.pgUsername, config.pgPassword)
    logInfo("[DorisMetaConnection] Connected to PostgreSQL")
    conn
  }

  def getConnection: Connection = synchronized {
    if (!_initialized || _connection.isClosed || !_connection.isValid(10)) {
      _connection = connect()
      _initialized = true
    }
    _connection
  }

  override def close(): Unit = synchronized {
    if (_initialized && !_connection.isClosed) {
      _connection.close()
      _initialized = false
    }
  }
}
