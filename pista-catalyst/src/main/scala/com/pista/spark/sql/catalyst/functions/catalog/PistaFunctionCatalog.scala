package com.pista.spark.sql.catalyst.functions.catalog

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.aggregate.{Roaring64Build, Roaring64UnionAggregate, VectorCentroid}
import com.pista.spark.sql.catalyst.functions.scalar._
import com.pista.spark.sql.catalyst.functions.table.Roaring64Explode
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object PistaFunctionCatalog {
  private val OwnerClass = getClass.getName

  val definitions: Vector[FunctionDefinition] = Vector(
    scalar("pista_ip_to_binary", FunctionCategory.Network, NetworkExpressions.ipToBinary,
      args("ip" -> "STRING"), "BINARY", "Converts an IPv4 or IPv6 literal to network-order bytes."),
    scalar("pista_try_ip_to_binary", FunctionCategory.Network, NetworkExpressions.tryIpToBinary,
      args("ip" -> "STRING"), "BINARY", "Converts a valid IP literal and returns null for malformed input."),
    scalar("pista_binary_to_ip", FunctionCategory.Network, NetworkExpressions.binaryToIp,
      args("ip" -> "BINARY"), "STRING", "Renders 4-byte or 16-byte network-order addresses."),
    scalar("pista_ipv4_to_long", FunctionCategory.Network, NetworkExpressions.ipv4ToLong,
      args("ip" -> "STRING"), "BIGINT", "Converts an IPv4 literal to an unsigned 32-bit numeric key."),
    scalar("pista_long_to_ipv4", FunctionCategory.Network, NetworkExpressions.longToIpv4,
      args("value" -> "BIGINT"), "STRING", "Renders an unsigned 32-bit numeric key as IPv4."),
    scalar("pista_ip_family", FunctionCategory.Network, NetworkExpressions.ipFamily,
      args("ip" -> "STRING"), "INT", "Returns 4 or 6 for a valid address literal."),
    scalar("pista_ip_is_private", FunctionCategory.Network, NetworkExpressions.ipIsPrivate,
      args("ip" -> "STRING"), "BOOLEAN", "Tests RFC 1918 IPv4 and RFC 4193 IPv6 private ranges."),
    scalar("pista_ip_in_cidr", FunctionCategory.Network, NetworkExpressions.ipInCidr,
      args("ip" -> "STRING", "cidr" -> "STRING"), "BOOLEAN", "Tests strict address membership in canonical CIDR."),
    scalar("pista_ip_prefix", FunctionCategory.Network, NetworkExpressions.ipPrefix,
      args("ip" -> "STRING", "prefix_length" -> "INT"), "BINARY", "Masks an address to a network prefix."),

    scalar("pista_try_parse_date", FunctionCategory.DateTime, DateTimeExpressions.tryParseDate,
      args("value" -> "STRING", "formats" -> "ARRAY<STRING>"), "DATE", "Parses a date using ordered caller-provided formats.", Set("formats")),
    scalar("pista_try_parse_timestamp", FunctionCategory.DateTime, DateTimeExpressions.tryParseTimestamp,
      args("value" -> "STRING", "formats" -> "ARRAY<STRING>", "timezone" -> "STRING"), "TIMESTAMP",
      "Parses a local timestamp with ordered formats and an explicit timezone.", Set("formats", "timezone")),

    aggregate("pista_roaring64_build", FunctionCategory.Roaring64, aggregateBuilder("pista_roaring64_build", children => Roaring64Build(children.head)),
      args("value" -> "BIGINT"), "BINARY", "Builds a portable versioned Roaring64 cohort."),
    aggregate("pista_roaring64_union_agg", FunctionCategory.Roaring64, aggregateBuilder("pista_roaring64_union_agg", children => Roaring64UnionAggregate(children.head)),
      args("bitmap" -> "BINARY"), "BINARY", "Unions versioned Roaring64 cohorts across rows."),
    scalar("pista_roaring64_from_array", FunctionCategory.Roaring64, Roaring64Expressions.fromArray,
      args("values" -> "ARRAY<BIGINT>"), "BINARY", "Builds a versioned Roaring64 cohort from one array."),
    scalar("pista_roaring64_union", FunctionCategory.Roaring64, Roaring64Expressions.union,
      args("left" -> "BINARY", "right" -> "BINARY"), "BINARY", "Returns the union of two cohorts."),
    scalar("pista_roaring64_intersect", FunctionCategory.Roaring64, Roaring64Expressions.intersect,
      args("left" -> "BINARY", "right" -> "BINARY"), "BINARY", "Returns the intersection of two cohorts."),
    scalar("pista_roaring64_xor", FunctionCategory.Roaring64, Roaring64Expressions.xor,
      args("left" -> "BINARY", "right" -> "BINARY"), "BINARY", "Returns the symmetric difference of two cohorts."),
    scalar("pista_roaring64_and_not", FunctionCategory.Roaring64, Roaring64Expressions.andNot,
      args("left" -> "BINARY", "right" -> "BINARY"), "BINARY", "Subtracts the right cohort from the left cohort."),
    scalar("pista_roaring64_cardinality", FunctionCategory.Roaring64, Roaring64Expressions.cardinality,
      args("bitmap" -> "BINARY"), "BIGINT", "Returns the exact cohort cardinality."),
    scalar("pista_roaring64_contains", FunctionCategory.Roaring64, Roaring64Expressions.contains,
      args("bitmap" -> "BINARY", "value" -> "BIGINT"), "BOOLEAN", "Tests membership in a cohort."),
    table("pista_roaring64_explode", FunctionCategory.Roaring64, Roaring64Explode.builder,
      args("bitmap" -> "BINARY", "max_rows" -> "INT"), StructType(Seq(StructField("value", LongType, nullable = false))),
      "Expands a validated cohort into bounded ascending rows.", Set("max_rows")),

    scalar("pista_json_is_valid", FunctionCategory.Json, JsonUrlExpressions.jsonIsValid,
      args("json" -> "STRING"), "BOOLEAN", "Tests JSON syntax without asserting a schema."),
    scalar("pista_json_merge_patch", FunctionCategory.Json, JsonUrlExpressions.jsonMergePatch,
      args("target" -> "STRING", "patch" -> "STRING"), "STRING", "Applies an RFC 7396 JSON Merge Patch."),

    scalar("pista_registrable_domain", FunctionCategory.UrlDomain, JsonUrlExpressions.registrableDomain,
      args("domain" -> "STRING"), "STRING", "Returns the registrable domain using Pista's fixed Public Suffix List snapshot."),
    scalar("pista_try_url_decode", FunctionCategory.UrlDomain, JsonUrlExpressions.tryUrlDecode,
      args("value" -> "STRING"), "STRING", "Decodes UTF-8 form URL encoding and returns null on malformed input."),

    scalar("pista_vector_inner_product", FunctionCategory.Vector, VectorExpressions.innerProduct,
      args("left" -> "ARRAY<DOUBLE>", "right" -> "ARRAY<DOUBLE>"), "DOUBLE", "Computes a strict finite vector inner product."),
    scalar("pista_vector_l2_distance", FunctionCategory.Vector, VectorExpressions.l2Distance,
      args("left" -> "ARRAY<DOUBLE>", "right" -> "ARRAY<DOUBLE>"), "DOUBLE", "Computes a stable Euclidean distance."),
    scalar("pista_vector_cosine_similarity", FunctionCategory.Vector, VectorExpressions.cosineSimilarity,
      args("left" -> "ARRAY<DOUBLE>", "right" -> "ARRAY<DOUBLE>"), "DOUBLE", "Computes cosine similarity for non-zero vectors."),
    scalar("pista_vector_l2_normalize", FunctionCategory.Vector, VectorExpressions.l2Normalize,
      args("value" -> "ARRAY<DOUBLE>"), "ARRAY<DOUBLE>", "Returns a unit-length vector."),
    aggregate("pista_vector_centroid", FunctionCategory.Vector, aggregateBuilder("pista_vector_centroid", children => VectorCentroid(children.head)),
      args("value" -> "ARRAY<DOUBLE>"), "ARRAY<DOUBLE>", "Computes a compensated per-group vector centroid.")
  ).sortBy(_.canonicalName)

  val digest: String = FunctionManifest.digest(definitions)

  private val duplicateNames = definitions.groupBy(_.canonicalName).collect {
    case (name, values) if values.size != 1 => name
  }
  require(duplicateNames.isEmpty, s"duplicate Pista function names: ${duplicateNames.mkString(",")}")
  require(definitions.size == 30, s"expected 30 Pista functions, got ${definitions.size}")
  require(definitions.count(_.kind == FunctionKind.Scalar) == 26, "expected 26 scalar functions")
  require(definitions.count(_.kind == FunctionKind.Aggregate) == 3, "expected 3 aggregate functions")
  require(definitions.count(_.kind == FunctionKind.Table) == 1, "expected 1 table function")
  require(definitions.forall(_.canonicalName.startsWith("pista_")), "all public functions must use the pista_ prefix")

  private def args(values: (String, String)*): Seq[FunctionArgument] =
    values.map { case (name, dataType) => FunctionArgument(name, dataType) }

  private def scalar(
      name: String,
      category: FunctionCategory,
      builder: FunctionRegistry.FunctionBuilder,
      arguments: Seq[FunctionArgument],
      returnType: String,
      summary: String,
      foldable: Set[String] = Set.empty): ScalarFunctionDefinition = {
    val signature = FunctionSignature(
      arguments.map(argument => argument.copy(foldable = foldable.contains(argument.name))), returnType)
    val documentation = docs(name, arguments, summary)
    val digest = FunctionManifest.contractDigest(
      name, category, FunctionKind.Scalar, Seq(signature), None)
    ScalarFunctionDefinition(name, category, Seq(signature), documentation, None, digest,
      expressionInfo(name, summary, arguments, category, digest, FunctionKind.Scalar), builder)
  }

  private def aggregate(
      name: String,
      category: FunctionCategory,
      builder: FunctionRegistry.FunctionBuilder,
      arguments: Seq[FunctionArgument],
      returnType: String,
      summary: String): AggregateFunctionDefinition = {
    val signature = FunctionSignature(arguments, returnType)
    val documentation = docs(name, arguments, summary)
    val digest = FunctionManifest.contractDigest(
      name, category, FunctionKind.Aggregate, Seq(signature), None)
    AggregateFunctionDefinition(name, category, Seq(signature), documentation, None, digest,
      expressionInfo(name, summary, arguments, category, digest, FunctionKind.Aggregate), builder)
  }

  private def table(
      name: String,
      category: FunctionCategory,
      builder: org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry.TableFunctionBuilder,
      arguments: Seq[FunctionArgument],
      outputSchema: StructType,
      summary: String,
      foldable: Set[String]): TableFunctionDefinition = {
    val signature = FunctionSignature(
      arguments.map(argument => argument.copy(foldable = foldable.contains(argument.name))), "TABLE")
    val documentation = docs(name, arguments, summary)
    val digest = FunctionManifest.contractDigest(
      name, category, FunctionKind.Table, Seq(signature), Some(outputSchema.json))
    TableFunctionDefinition(name, category, Seq(signature), outputSchema, documentation, None, digest,
      expressionInfo(name, summary, arguments, category, digest, FunctionKind.Table), builder)
  }

  private def aggregateBuilder(
      name: String,
      create: Seq[org.apache.spark.sql.catalyst.expressions.Expression] => org.apache.spark.sql.catalyst.expressions.Expression)
      : FunctionRegistry.FunctionBuilder = children => {
    if (children.length != 1)
      throw PistaUDFErrors.udfArgumentLengthMismatchError(name, 1, children.length)
    create(children)
  }

  private def docs(
      name: String,
      arguments: Seq[FunctionArgument],
      summary: String): FunctionDocumentation =
    FunctionDocumentation(
      summary,
      arguments.map(argument => s"${argument.name}: ${argument.dataType}").mkString("; "),
      s"SELECT $name(${arguments.map(argument => s"<${argument.name}>").mkString(", ")})")

  private def expressionInfo(
      name: String,
      summary: String,
      arguments: Seq[FunctionArgument],
      category: FunctionCategory,
      contractDigest: String,
      kind: FunctionKind): ExpressionInfo = {
    val group = kind match {
      case FunctionKind.Aggregate => "agg_funcs"
      case FunctionKind.Table => "generator_funcs"
      case _ => category match {
        case FunctionCategory.DateTime => "datetime_funcs"
        case FunctionCategory.Json => "json_funcs"
        case FunctionCategory.UrlDomain => "url_funcs"
        case _ => "misc_funcs"
      }
    }
    new ExpressionInfo(
      s"$OwnerClass:$contractDigest",
      null,
      name,
      s"_FUNC_(${arguments.map(_.name).mkString(", ")}) - $summary",
      "",
      s"\n    Examples:\n      > SELECT _FUNC_(${arguments.map(_ => "...").mkString(", ")});\n",
      "",
      group,
      "",
      "",
      "")
  }
}
