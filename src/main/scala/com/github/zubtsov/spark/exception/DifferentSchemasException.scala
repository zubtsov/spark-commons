package com.github.zubtsov.spark.exception

import com.github.zubtsov.spark.DataFrameComparison.SchemaDifference

final case class DifferentSchemasException(schemaDifference: SchemaDifference) extends Exception {
  override def getMessage: String = s"\nMissing fields from the left ${schemaDifference.leftMissingFields.mkString}" +
    s"\nMissing fields from the right ${schemaDifference.rightMissingFields.mkString}"
}
