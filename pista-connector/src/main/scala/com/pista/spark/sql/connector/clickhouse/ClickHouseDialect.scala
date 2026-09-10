package com.pista.spark.sql.connector.clickhouse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp, Types}
import java.util.Locale
import scala.util.matching.Regex

/**
 * ClickHouse SQL Dialect
 *
 * Adds ClickHouse handling for Nullable, nested arrays, and high-precision decimals.
 * Register before use with JdbcDialects.registerDialect(ClickHouseDialect).
 */
object ClickHouseDialect extends JdbcDialect with Logging {

  private val arrayTypePattern: Regex    = "^Array\\((.*)\\)$".r
  private val dateTypePattern: Regex     = "^[dD][aA][tT][eE]$".r
  private val dateTimeTypePattern: Regex = "^[dD][aA][tT][eE][tT][iI][mM][eE](64)?(\\((.*)\\))?$".r
  private val decimalTypePattern: Regex  = "^[dD][eE][cC][iI][mM][aA][lL]\\((\\d+),\\s*(\\d+)\\)$".r
  private val decimalTypePattern2: Regex = "^[dD][eE][cC][iI][mM][aA][lL](32|64|128|256)\\((\\d+)\\)$".r
  private val enumTypePattern: Regex     = "^Enum(8|16)$".r
  private val fixedStringTypePattern: Regex = "^FixedString\\((\\d+)\\)$".r
  private val nullableTypePattern: Regex = "^Nullable\\((.*)\\)".r

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:clickhouse")

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    val scale = md.build.getLong("scale").toInt
    sqlType match {
      case Types.ARRAY =>
        unwrapNullable(typeName) match {
          case (_, arrayTypePattern(nestType)) =>
            toCatalystType(nestType, size, scale)
              .map { case (nullable, dt) => ArrayType(dt, nullable) }
          case _ => None
        }
      case _ => toCatalystType(typeName, size, scale).map(_._2)
    }
  }

  private def toCatalystType(
      typeName: String,
      precision: Int,
      scale: Int): Option[(Boolean, DataType)] = {
    val (nullable, t) = unwrapNullable(typeName)
    val dt = t match {
      case "String" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) => Some(StringType)
      case "Int8"                                            => Some(ByteType)
      case "UInt8" | "Int16"                                 => Some(ShortType)
      case "UInt16" | "Int32"                                => Some(IntegerType)
      case "UInt32" | "Int64" | "UInt64" | "IPv4"           => Some(LongType)
      case "Int128" | "Int256" | "UInt256"                   => None
      case "Float32"                                         => Some(FloatType)
      case "Float64"                                         => Some(DoubleType)
      case dateTypePattern()                                 => Some(DateType)
      case dateTimeTypePattern()                             => Some(TimestampType)
      case decimalTypePattern(p, s)  => Some(DecimalType(p.toInt, s.toInt))
      case decimalTypePattern2(w, s) => w match {
        case "32"  => Some(DecimalType(9, s.toInt))
        case "64"  => Some(DecimalType(18, s.toInt))
        case "128" => Some(DecimalType(38, s.toInt))
        case "256" => Some(DecimalType(76, s.toInt))
      }
      case _ => None
    }
    dt.map((nullable, _))
  }

  private def unwrapNullable(typeName: String): (Boolean, String) = typeName match {
    case nullableTypePattern(inner) => (true, inner)
    case _                          => (false, typeName)
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType    => Some(JdbcType("String", Types.VARCHAR))
    case BinaryType    => Some(JdbcType("String", Types.BINARY))
    case BooleanType   => Some(JdbcType("UInt8", Types.BOOLEAN))
    case ByteType      => Some(JdbcType("Int8", Types.TINYINT))
    case ShortType     => Some(JdbcType("Int16", Types.SMALLINT))
    case IntegerType   => Some(JdbcType("Int32", Types.INTEGER))
    case LongType      => Some(JdbcType("Int64", Types.BIGINT))
    case FloatType     => Some(JdbcType("Float", Types.FLOAT))
    case DoubleType    => Some(JdbcType("Double", Types.DOUBLE))
    case t: DecimalType => Some(JdbcType(s"Decimal(${t.precision},${t.scale})", Types.DECIMAL))
    case DateType      => Some(JdbcType("Date", Types.DATE))
    case TimestampType => Some(JdbcType("DateTime", Types.TIMESTAMP))
    case ArrayType(et, _) =>
      getJDBCType(et)
        .orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jt => JdbcType(s"Array(${jt.databaseTypeDefinition})", Types.ARRAY))
    case _ => None
  }

  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def isCascadingTruncateTable: Option[Boolean] = Some(false)

  override def compileValue(value: Any): Any = value match {
    case s: String    => s"'${escapeSql(s)}'"
    case t: Timestamp => s"'$t'"
    case d: Date      => s"'$d'"
    case a: Array[Any] => a.map(compileValue).mkString("[", ",", "]")
    case _            => value
  }
}
