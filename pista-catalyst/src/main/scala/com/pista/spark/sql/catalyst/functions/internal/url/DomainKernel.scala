package com.pista.spark.sql.catalyst.functions.internal.url

import com.pista.spark.errors.PistaUDFErrors
import com.pista.spark.sql.catalyst.functions.internal.network.NetworkKernel
import org.apache.spark.unsafe.types.UTF8String

import java.net.IDN
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.io.Source

object DomainKernel {
  private val ResourceName = "/com/pista/spark/sql/catalyst/functions/public_suffix_list.dat"
  private val MaxDomainLength = 253

  private final case class Rules(exact: Set[String], wildcard: Set[String], exception: Set[String])

  private lazy val rules: Rules = {
    val stream = Option(getClass.getResourceAsStream(ResourceName)).getOrElse(
      throw new IllegalStateException(s"missing Public Suffix List resource: $ResourceName"))
    val source = Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
    try {
      val lines = source.getLines().map(_.trim).filter(line => line.nonEmpty && !line.startsWith("//"))
      val exact = Set.newBuilder[String]
      val wildcard = Set.newBuilder[String]
      val exception = Set.newBuilder[String]
      lines.foreach { rule =>
        val value = rule.stripPrefix("!").stripPrefix("*.")
        val normalized = try IDN.toASCII(value, IDN.USE_STD3_ASCII_RULES).toLowerCase(Locale.ROOT)
        catch {
          // Java 17 implements IDNA 2003 and cannot encode a small set of newer Unicode PSL
          // labels. Retaining those rules in normalized Unicode form keeps the snapshot loadable;
          // user inputs unsupported by the runtime IDN implementation remain fail-closed.
          case _: IllegalArgumentException => value.toLowerCase(Locale.ROOT)
        }
        if (rule.startsWith("!")) exception += normalized
        else if (rule.startsWith("*.")) wildcard += normalized
        else exact += normalized
      }
      Rules(exact.result(), wildcard.result(), exception.result())
    } finally source.close()
  }

  def registrableDomain(input: UTF8String): UTF8String = {
    val domain = normalize(input.toString)
    if (looksLikeIpv4(domain) ||
        NetworkKernel.tryIpToBinary(UTF8String.fromString(domain)) != null) return null
    val labels = domain.split("\\.")

    val exceptionMatch = suffixes(labels).filter(rules.exception.contains).sortBy(labelCount).lastOption
    val publicSuffixLabels = exceptionMatch match {
      case Some(rule) => labelCount(rule) - 1
      case None =>
        val exactLengths = suffixes(labels).filter(rules.exact.contains).map(labelCount)
        val wildcardLengths = (1 until labels.length).flatMap { index =>
          val remainder = labels.drop(index).mkString(".")
          if (rules.wildcard.contains(remainder)) Some(labelCount(remainder) + 1) else None
        }
        (exactLengths ++ wildcardLengths).foldLeft(1)(Math.max)
    }

    if (labels.length <= publicSuffixLabels) null
    else UTF8String.fromString(labels.takeRight(publicSuffixLabels + 1).mkString("."))
  }

  private def normalize(value: String): String = {
    val withoutDot = if (value.endsWith(".")) value.dropRight(1) else value
    val ascii = try IDN.toASCII(withoutDot, IDN.USE_STD3_ASCII_RULES) catch {
      case _: IllegalArgumentException =>
        throw PistaUDFErrors.functionInvalidInputError(
          "pista_registrable_domain", "invalid internationalized domain name")
    }
    val normalized = ascii.toLowerCase(Locale.ROOT)
    if (normalized.isEmpty || normalized.length > MaxDomainLength ||
        normalized.startsWith(".") || normalized.endsWith(".") || normalized.contains(".."))
      throw PistaUDFErrors.functionInvalidInputError(
        "pista_registrable_domain", "invalid domain name")
    normalized
  }

  private def suffixes(labels: Array[String]): Seq[String] =
    labels.indices.map(index => labels.drop(index).mkString("."))

  private def looksLikeIpv4(value: String): Boolean = {
    val labels = value.split("\\.", -1)
    labels.length == 4 && labels.forall(label => label.nonEmpty && label.forall(_.isDigit))
  }

  private def labelCount(value: String): Int = value.count(_ == '.') + 1
}
