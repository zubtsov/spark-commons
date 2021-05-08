package com.github.zubtsov.spark.sql.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object IgnoreCaseComparison {
  private def lowered: Column => Column = transformString(c => lower(c))

  implicit class IgnoreCaseComparison(col: Column) {
    def ====(other: Any): Column = equalsIgnoreCase(other)

    def <==>(other: Any): Column = nullSafeEqualsIgnoreCase(other)

    def =!!=(other: Any): Column = notEqualsIgnoreCase(other)

    def equalsIgnoreCase(other: Any): Column = lowered(col) === lowered(lit(other))

    def nullSafeEqualsIgnoreCase(other: Any): Column = lowered(col) <=> lowered(lit(other))

    def notEqualsIgnoreCase(other: Any): Column = lowered(col) =!= lowered(lit(other))
  }

}
