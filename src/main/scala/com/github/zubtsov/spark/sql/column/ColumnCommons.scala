package com.github.zubtsov.spark.sql.column

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, Substring, SubstringIndex}

/**
 * More flexible signatures which are not provided out-of-box in [[org.apache.spark.sql.functions]]
 */
object ColumnCommons {
  private def withExpr(expr: Expression): Column = new Column(expr)

  def substring(str: Column, pos: Column, len: Column): Column = withExpr {
    Substring(str.expr, pos.expr, len.expr)
  }

  def substring_index(str: Column, delim: Column, count: Column): Column = withExpr {
    SubstringIndex(str.expr, delim.expr, count.expr)
  }
}
