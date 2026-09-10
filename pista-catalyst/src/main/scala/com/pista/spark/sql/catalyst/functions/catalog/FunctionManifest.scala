package com.pista.spark.sql.catalyst.functions.catalog

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import java.security.MessageDigest

object FunctionManifest {
  val SchemaVersion: Int = 1

  private val mapper = new ObjectMapper()
    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)

  def contractDigest(
      name: String,
      category: FunctionCategory,
      kind: FunctionKind,
      signatures: Seq[FunctionSignature],
      outputSchemaJson: Option[String]): String = {
    val node = mapper.createObjectNode()
    node.put("canonicalName", name)
    node.put("category", category.id)
    node.put("kind", kind.id)
    node.set("signatures", signaturesNode(signatures))
    outputSchemaJson.foreach { schema =>
      node.put("outputSchema", schema)
      ()
    }
    sha256(mapper.writeValueAsBytes(node))
  }

  def render(definitions: Seq[FunctionDefinition]): String = {
    val functions = mapper.createArrayNode()
    definitions.sortBy(_.canonicalName).foreach(definition => functions.add(definitionNode(definition)))

    val digestInput = mapper.createObjectNode()
    digestInput.put("schemaVersion", SchemaVersion)
    digestInput.set("functions", functions)
    val digest = sha256(mapper.writeValueAsBytes(digestInput))

    val root = mapper.createObjectNode()
    root.put("schemaVersion", SchemaVersion)
    root.put("digest", digest)
    root.set("functions", functions)
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n"
  }

  def digest(definitions: Seq[FunctionDefinition]): String = {
    val parsed = mapper.readTree(render(definitions))
    parsed.get("digest").asText()
  }

  private def definitionNode(definition: FunctionDefinition): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("canonicalName", definition.canonicalName)
    node.put("category", definition.category.id)
    node.put("kind", definition.kind.id)
    node.put("contractDigest", definition.contractDigest)
    node.set("signatures", signaturesNode(definition.signatures))
    definition match {
      case table: TableFunctionDefinition =>
        node.set[JsonNode]("outputSchema", mapper.readTree(table.outputSchema.json))
        ()
      case _ =>
    }
    node.set("documentation", documentationNode(definition.documentation))
    definition.since.foreach { since =>
      node.put("since", since)
      ()
    }
    node
  }

  private def signaturesNode(signatures: Seq[FunctionSignature]): ArrayNode = {
    val result = mapper.createArrayNode()
    signatures.foreach { signature =>
      val node = mapper.createObjectNode()
      val arguments = mapper.createArrayNode()
      signature.arguments.foreach { argument =>
        val argumentNode = mapper.createObjectNode()
        argumentNode.put("name", argument.name)
        argumentNode.put("dataType", argument.dataType)
        argumentNode.put("foldable", argument.foldable)
        arguments.add(argumentNode)
      }
      node.set("arguments", arguments)
      node.put("returnType", signature.returnType)
      result.add(node)
    }
    result
  }

  private def documentationNode(documentation: FunctionDocumentation): JsonNode = {
    val node = mapper.createObjectNode()
    node.put("summary", documentation.summary)
    node.put("arguments", documentation.arguments)
    node.put("examples", documentation.examples)
    node.put("notes", documentation.notes)
    node
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256")
      .digest(bytes)
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
}

/** Prints the canonical manifest for release tooling; the Catalog remains the source of truth. */
object FunctionManifestCli {
  def main(args: Array[String]): Unit =
    Console.print(FunctionManifest.render(PistaFunctionCatalog.definitions))
}
