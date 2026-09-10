package com.pista.spark.sql.functions

final case class FunctionInstallationReport(
    catalogDigest: String,
    installed: Vector[String],
    alreadyPresent: Vector[String]) {
  def installedCount: Int = installed.size
  def alreadyPresentCount: Int = alreadyPresent.size
  def totalCount: Int = installedCount + alreadyPresentCount
}
