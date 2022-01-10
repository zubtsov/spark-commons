package com.github.zubtsov.spark.exception

import com.github.zubtsov.spark.DataFrameComparison.ColumnNamesDifference

final case class DifferentColumnNamesException(columnNamesDifference: ColumnNamesDifference) extends Exception {
  override def getMessage: String = s"\nMissing columns from the left ${columnNamesDifference.leftMissingCols.mkString(", ")}" +
    s"\nMissing columns from the right ${columnNamesDifference.rightMissingCols.mkString(", ")}"
}
