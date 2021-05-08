package com.github.zubtsov.spark.sql.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object IgnoreCaseAndWhitespaceComparison {
  private def trimmedAndLowered: Column => Column = transformString(c => lower(trim(c)))

  implicit class IgnoreCaseAndWhitespaceComparison(col: Column) {
    def ====(other: Any): Column = equalsIgnoreCaseAndWhitespaces(other)

    def <==>(other: Any): Column = nullSafeEqualsIgnoreCaseAndWhitespaces(other)

    def =!!=(other: Any): Column = notEqualsIgnoreCaseAndWhitespaces(other)

    def equalsIgnoreCaseAndWhitespaces(other: Any): Column =
      trimmedAndLowered(col) === trimmedAndLowered(lit(other))

    def nullSafeEqualsIgnoreCaseAndWhitespaces(other: Any): Column =
      trimmedAndLowered(col) <=> trimmedAndLowered(lit(other))

    def notEqualsIgnoreCaseAndWhitespaces(other: Any): Column =
      trimmedAndLowered(col) =!= trimmedAndLowered(lit(other))
  }

}
