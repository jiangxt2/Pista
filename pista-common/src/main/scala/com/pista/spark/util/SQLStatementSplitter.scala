package com.pista.spark.util

import scala.collection.mutable.ArrayBuffer

/**
 * SQL statement splitter, state machine correctly handles semicolons in comments and strings
 *
 */

object SQLStatementSplitter {

  /**
   * Split multi-statement SQL by semicolons
   *
   * Implementation:
   * - Use state machine to traverse SQL text character by character
   * - Maintain quote, comment, escape, and other states
   * - split at semicolons not enclosed in quotes or comments
   * - Filter out empty statements and pure comments automatically
   *
   * @param sql Original SQL text
   * @return Array of split SQL statements
   */
  def split(sql: String): Array[String] = {
    if (sql == null || sql.isEmpty) {
      return Array.empty[String]
    }

    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideSimpleComment = false
    var bracketedCommentLevel = 0
    var escape = false
    var beginIndex = 0
    var leavingBracketedComment = false
    var isStatement = false
    val ret = ArrayBuffer[String]()

    def insideBracketedComment: Boolean = bracketedCommentLevel > 0

    def insideComment: Boolean = insideSimpleComment || insideBracketedComment

    def statementInProgress(index: Int): Boolean =
      isStatement || (!insideComment &&
        index > beginIndex &&
        s"${sql.charAt(index)}".trim.nonEmpty)

    for (index <- 0 until sql.length) {
      // Check if block comment level needs to be decremented
      // The closing slash still belongs to the block comment, so leave it on the next iteration.
      if (leavingBracketedComment) {
        bracketedCommentLevel -= 1
        leavingBracketedComment = false
      }

      if (sql.charAt(index) == '\'' && !insideComment) {
        // Process single quotes
        // Reference SPARK-31595: If not switching to single quote state when in double quotes or escaping state
        if (!escape && !insideDoubleQuote)
          insideSingleQuote = !insideSingleQuote
      } else if (sql.charAt(index) == '\"' && !insideComment) {
        // Process double quotes
        // Reference SPARK-31595: If not switching to the double quote state when in single quote mode or in escape state
        if (!escape && !insideSingleQuote)
          insideDoubleQuote = !insideDoubleQuote
      } else if (sql.charAt(index) == '-') {
        val hasNext = index + 1 < sql.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Within quotes or comments, ignore '-'
          // Avoid starting a new comment when quotes are within quotes or already in a comment.
          // Example: select "quoted value --"
          //                           ^^ Disable comments within quotes.
        }
        else if (hasNext && sql.charAt(index + 1) == '-')
          // Encounter '--', enter line comment
          insideSimpleComment = true
      } else if (sql.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // Do not split inside quotes or comments.
        }
        else {
          if (isStatement)
            // Splitting, excluding the semicolon itself.
            ret += sql.substring(beginIndex, index)
          beginIndex = index + 1
          isStatement = false
        }
      } else if (sql.charAt(index) == '\n') {
        // newline ends comment
        if (!escape)
          insideSimpleComment = false
      } else if (sql.charAt(index) == '/' && !insideSimpleComment) {
        val hasNext = index + 1 < sql.length
        if (insideSingleQuote || insideDoubleQuote) {
          // Within quotes, ignore '/'
        } else if (insideBracketedComment && index > 0 && sql.charAt(index - 1) == '*') {
          // Encounter '*/', mark that it is about to leave block comments
          // Decrease bracketedCommentLevel only at the start of the next iteration
          leavingBracketedComment = true
        } else if (hasNext && sql.charAt(index + 1) == '*') {
          // Encounter '/*', enter block comment
          bracketedCommentLevel += 1
        }
      }

      // Handle escaped characters
      if (escape) {
        escape = false
      } else if (sql.charAt(index) == '\\') {
        escape = true
      }

      isStatement = statementInProgress(index)
    }

    // Check if the last character is the end of a nested block comment.
    val endOfBracketedComment = leavingBracketedComment && bracketedCommentLevel == 1

    // Spark SQL supports single-line comments and nested block comments.
    // But if Spark SQL receives only one comment, it throws a parsing exception.
    // In Spark SQL CLI, if the entire query ends with a complete comment,
    // CLI should ignore this comment.
    // If there are unfinished statements or incomplete block comments at the end.
    // CLI should pass this to the backend engine, which may throw a clear error message.
    if (!endOfBracketedComment && (isStatement || insideBracketedComment)) {
      ret += sql.substring(beginIndex)
    }

    ret.toArray
  }
}
