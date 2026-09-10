package com.pista.spark.util

import com.pista.spark.errors.{PistaConfigException, PistaErrors}

/**
 **parser for parameter types**
 *
 * the corresponding Scala/Java type declareday declared Scala/Java type,
 * Use Spark native parameterized query `spark.sql(sqlText, args: Map[String, Any])`.
 *
 */
object ParamTypeParser {

  /**
   * Parse parameter values by declaration type.
   *
   * @param value        String value of the parameter
   * @param declaredType Declaration type (default is String when not declared)
   * @return Type-annotated parsed values
   */
  def parseTypedValue(value: String, declaredType: Option[String]): Any =
    try {
      declaredType.map(_.toLowerCase) match {
        case Some("int")       => value.toInt
        case Some("long")      => value.toLong
        case Some("double")    => value.toDouble
        case Some("boolean")   => value.toBoolean
        case Some("date")      => java.sql.Date.valueOf(value)
        case Some("timestamp") => java.sql.Timestamp.valueOf(value)
        case Some(unknown)     =>
          throw PistaErrors.invalidParamTypeError(unknown)
        case _                 => value
      }
    } catch {
      case e: PistaConfigException => throw e
      case e: Exception =>
        val reason = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        throw PistaErrors.paramTypeConversionError(
          value, declaredType.getOrElse("string"), reason)
    }
}
