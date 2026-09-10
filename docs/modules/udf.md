# Catalyst Function Catalog

`pista-catalyst` contains the single JVM definition of Pista SQL functions. `pista-sql` installs the catalog into a SparkSession.

## Catalog

The current public manifest contains 30 functions: 26 scalar functions, three aggregate functions, and one table function. The generated manifest and `PistaFunctionCatalog` are authoritative for names and signatures.

Function groups include:

- network and IP conversion;
- date and timestamp parsing;
- URL decoding and registrable domains;
- JSON validation and merge patch;
- Roaring64 bitmap operations;
- vector distance, similarity, normalization, and centroid operations.

<!-- BEGIN GENERATED FUNCTION CATALOG -->
| Type | function | signature | purpose |
|---|---|---|---|
| Scalar | `pista_binary_to_ip` | `(BINARY) -> STRING` | Renders 4-byte or 16-byte network-order addresses. |
| Scalar | `pista_ip_family` | `(STRING) -> INT` | Returns 4 or 6 for a valid address literal. |
| Scalar | `pista_ip_in_cidr` | `(STRING, STRING) -> BOOLEAN` | Tests strict address membership in canonical CIDR. |
| Scalar | `pista_ip_is_private` | `(STRING) -> BOOLEAN` | Tests RFC 1918 IPv4 and RFC 4193 IPv6 private ranges. |
| Scalar | `pista_ip_prefix` | `(STRING, INT) -> BINARY` | Masks an address to a network prefix. |
| Scalar | `pista_ip_to_binary` | `(STRING) -> BINARY` | Converts an IPv4 or IPv6 literal to network-order bytes. |
| Scalar | `pista_ipv4_to_long` | `(STRING) -> BIGINT` | Converts an IPv4 literal to an unsigned 32-bit numeric key. |
| Scalar | `pista_json_is_valid` | `(STRING) -> BOOLEAN` | Tests JSON syntax without asserting a schema. |
| Scalar | `pista_json_merge_patch` | `(STRING, STRING) -> STRING` | Applies an RFC 7396 JSON Merge Patch. |
| Scalar | `pista_long_to_ipv4` | `(BIGINT) -> STRING` | Renders an unsigned 32-bit numeric key as IPv4. |
| Scalar | `pista_registrable_domain` | `(STRING) -> STRING` | Returns the registrable domain using Pista's fixed Public Suffix List snapshot. |
| Scalar | `pista_roaring64_and_not` | `(BINARY, BINARY) -> BINARY` | Subtracts the right cohort from the left cohort. |
| UDAF | `pista_roaring64_build` | `(BIGINT) -> BINARY` | Builds a portable versioned Roaring64 cohort. |
| Scalar | `pista_roaring64_cardinality` | `(BINARY) -> BIGINT` | Returns the exact cohort cardinality. |
| Scalar | `pista_roaring64_contains` | `(BINARY, BIGINT) -> BOOLEAN` | Tests membership in a cohort. |
| UDTF | `pista_roaring64_explode` | `(BINARY, INT) -> TABLE<value BIGINT>` | Expands a validated cohort into bounded ascending rows. |
| Scalar | `pista_roaring64_from_array` | `(ARRAY<BIGINT>) -> BINARY` | Builds a versioned Roaring64 cohort from one array. |
| Scalar | `pista_roaring64_intersect` | `(BINARY, BINARY) -> BINARY` | Returns the intersection of two cohorts. |
| Scalar | `pista_roaring64_union` | `(BINARY, BINARY) -> BINARY` | Returns the union of two cohorts. |
| UDAF | `pista_roaring64_union_agg` | `(BINARY) -> BINARY` | Unions versioned Roaring64 cohorts across rows. |
| Scalar | `pista_roaring64_xor` | `(BINARY, BINARY) -> BINARY` | Returns the symmetric difference of two cohorts. |
| Scalar | `pista_try_ip_to_binary` | `(STRING) -> BINARY` | Converts a valid IP literal and returns null for malformed input. |
| Scalar | `pista_try_parse_date` | `(STRING, ARRAY<STRING>) -> DATE` | Parses a date using ordered caller-provided formats. |
| Scalar | `pista_try_parse_timestamp` | `(STRING, ARRAY<STRING>, STRING) -> TIMESTAMP` | Parses a local timestamp with ordered formats and an explicit timezone. |
| Scalar | `pista_try_url_decode` | `(STRING) -> STRING` | Decodes UTF-8 form URL encoding and returns null on malformed input. |
| UDAF | `pista_vector_centroid` | `(ARRAY<DOUBLE>) -> ARRAY<DOUBLE>` | Computes a compensated per-group vector centroid. |
| Scalar | `pista_vector_cosine_similarity` | `(ARRAY<DOUBLE>, ARRAY<DOUBLE>) -> DOUBLE` | Computes cosine similarity for non-zero vectors. |
| Scalar | `pista_vector_inner_product` | `(ARRAY<DOUBLE>, ARRAY<DOUBLE>) -> DOUBLE` | Computes a strict finite vector inner product. |
| Scalar | `pista_vector_l2_distance` | `(ARRAY<DOUBLE>, ARRAY<DOUBLE>) -> DOUBLE` | Computes a stable Euclidean distance. |
| Scalar | `pista_vector_l2_normalize` | `(ARRAY<DOUBLE>) -> ARRAY<DOUBLE>` | Returns a unit-length vector. |
<!-- END GENERATED FUNCTION CATALOG -->

## Installation

Use `PistaSparkSessionExtensions` at session creation or call the installer before using the catalog. Conflicts fail closed; partial installation must roll back instead of leaving an unpredictable function set.

Clients other than the JVM may use Spark-native APIs after the Pista catalog is installed. Pista does not ship a Python wrapper or Python client contract.

## Development contract

New functions must provide a stable name, kind, signature, return type, documentation, null behavior, error semantics, and generated manifest entry. Tests cover SQL registration, optimizer behavior, interpreted/code-generated execution, nulls, invalid input, aggregates, and table-function row expansion.

Do not add Python or Scala UDF fallbacks for performance-sensitive public functions.
