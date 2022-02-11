package com.github.zubtsov.spark.exception

import com.github.zubtsov.spark.exception.EmptyRowException.{
  bothDatasetsHaveNullRows,
  leftDatasetHasNullRows,
  rightDatasetHasNullRows
}

class EmptyRowException(hasNullRowsLeft: Boolean, hasNullRowsRight: Boolean) extends Exception {
  override def getMessage: String =
    if (hasNullRowsLeft && hasNullRowsRight) bothDatasetsHaveNullRows
    else if (hasNullRowsRight) rightDatasetHasNullRows
    else leftDatasetHasNullRows
}
object EmptyRowException {
  private val leftDatasetHasNullRows   = "The LEFT dataset has empty rows."
  private val rightDatasetHasNullRows  = "The RIGHT dataset has empty rows."
  private val bothDatasetsHaveNullRows = "BOTH datasets have empty rows."
}
