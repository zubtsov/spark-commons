package com.github.zubtsov.spark.sql.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object IgnoreWhitespaceComparison {
  private def trimmed: Column => Column = transformString(c => trim(c))

  implicit class IgnoreWhitespaceComparison(col: Column) {
    def ====(other: Any): Column = equalsIgnoreWhitespaces(other)

    def <==>(other: Any): Column = nullSafeEqualsIgnoreWhitespaces(other)

    def =!!=(other: Any): Column = notEqualsIgnoreWhitespaces(other)

    def equalsIgnoreWhitespaces(other: Any): Column = trimmed(col) === trimmed(lit(other))

    def nullSafeEqualsIgnoreWhitespaces(other: Any): Column = trimmed(col) <=> trimmed(lit(other))

    def notEqualsIgnoreWhitespaces(other: Any): Column = trimmed(col) =!= trimmed(lit(other))
  }

}
