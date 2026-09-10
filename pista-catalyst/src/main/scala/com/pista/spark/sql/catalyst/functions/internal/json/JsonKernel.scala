package com.pista.spark.sql.catalyst.functions.internal.json

import com.fasterxml.jackson.core.{JsonFactoryBuilder, StreamReadConstraints}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.pista.spark.errors.PistaUDFErrors
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object JsonKernel {
  private val MaxInputBytes = 4 * 1024 * 1024
  private val MaxOutputBytes = 8 * 1024 * 1024
  private val MaxDepth = 128
  private val Factory = new JsonFactoryBuilder()
    .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(MaxDepth).build())
    .build()
  private val Mapper = new ObjectMapper(Factory)

  def isValid(value: UTF8String): java.lang.Boolean = {
    if (value.numBytes() > MaxInputBytes) java.lang.Boolean.FALSE
    else {
      try {
        val parser = Factory.createParser(value.getBytes)
        try {
          val first = parser.nextToken()
          val valid = first != null && {
            Mapper.readTree[JsonNode](parser)
            parser.nextToken() == null
          }
          java.lang.Boolean.valueOf(valid)
        } finally parser.close()
      } catch { case _: Exception => java.lang.Boolean.FALSE }
    }
  }

  def mergePatch(target: UTF8String, patch: UTF8String): UTF8String = {
    val targetNode = parse("pista_json_merge_patch", target)
    val patchNode = parse("pista_json_merge_patch", patch)
    val merged = applyPatch(targetNode, patchNode)
    val result = Mapper.writeValueAsBytes(merged)
    if (result.length > MaxOutputBytes)
      throw PistaUDFErrors.functionResourceLimitError(
        "pista_json_merge_patch", "output bytes", MaxOutputBytes, result.length)
    UTF8String.fromBytes(result)
  }

  private def parse(functionName: String, value: UTF8String): JsonNode = {
    if (value.numBytes() > MaxInputBytes)
      throw PistaUDFErrors.functionResourceLimitError(
        functionName, "input bytes", MaxInputBytes, value.numBytes())
    val parsed = try Mapper.readTree(value.getBytes) catch {
      case exception: Exception =>
        throw PistaUDFErrors.functionInvalidInputError(
          functionName, s"invalid JSON: ${Option(exception.getMessage).getOrElse("parse failure")}")
    }
    if (parsed == null || parsed.isMissingNode)
      throw PistaUDFErrors.functionInvalidInputError(functionName, "invalid JSON: empty input")
    parsed
  }

  private def applyPatch(target: JsonNode, patch: JsonNode): JsonNode = {
    if (!patch.isObject) patch.deepCopy()
    else {
      val result = if (target != null && target.isObject) target.deepCopy[ObjectNode]() else Mapper.createObjectNode()
      patch.fields().asScala.foreach { entry =>
        if (entry.getValue.isNull) result.remove(entry.getKey)
        else result.set[JsonNode](entry.getKey, applyPatch(result.get(entry.getKey), entry.getValue))
      }
      result
    }
  }
}
