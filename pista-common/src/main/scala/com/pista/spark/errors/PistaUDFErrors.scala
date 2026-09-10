package com.pista.spark.errors

/**
 * Pista UDF error class generation
 *
 * Throw custom exception class PistaUDFException
 * Error classes are defined in pista-common/error-classes.json, including the imported Spark catalog.
 *
 */
object PistaUDFErrors {

  def udfArgumentLengthMismatchError(
      udfName: String,
      expected: Int,
      actual: Int): PistaUDFException = {
    new PistaUDFException(
      "PISTA_UDF_ARGUMENT_LENGTH_MISMATCH",
      Map(
        "udfName" -> udfName,
        "expected" -> expected.toString,
        "actual" -> actual.toString))
  }

  def udfInvalidArgumentTypeError(
      udfName: String,
      argIndex: Int,
      actualType: String,
      expectedType: String): PistaUDFException = {
    new PistaUDFException(
      "PISTA_UDF_INVALID_ARGUMENT_TYPE",
      Map(
        "udfName" -> udfName,
        "argIndex" -> argIndex.toString,
        "actualType" -> actualType,
        "expectedType" -> expectedType))
  }

  def functionInvalidInputError(functionName: String, reason: String): PistaUDFException =
    new PistaUDFException(
      "PISTA_FUNCTION_INVALID_INPUT",
      Map("functionName" -> functionName, "reason" -> reason))

  def functionResourceLimitError(
      functionName: String,
      resource: String,
      limit: Long,
      actual: Long): PistaUDFException =
    new PistaUDFException(
      "PISTA_FUNCTION_RESOURCE_LIMIT_EXCEEDED",
      Map(
        "functionName" -> functionName,
        "resource" -> resource,
        "limit" -> limit.toString,
        "actual" -> actual.toString))

  def functionBinaryFormatError(functionName: String, reason: String): PistaUDFException =
    new PistaUDFException(
      "PISTA_FUNCTION_INVALID_BINARY_FORMAT",
      Map("functionName" -> functionName, "reason" -> reason))

  def functionCatalogConflictError(
      functionName: String,
      expectedOwner: String,
      actualOwner: String): PistaUDFException =
    new PistaUDFException(
      "PISTA_FUNCTION_CATALOG_CONFLICT",
      Map(
        "functionName" -> functionName,
        "expectedOwner" -> expectedOwner,
        "actualOwner" -> actualOwner))

  def functionInstallationError(reason: String, cause: Throwable = null): PistaUDFException =
    new PistaUDFException(
      "PISTA_FUNCTION_INSTALLATION_FAILED",
      Map("reason" -> reason),
      cause)
}
