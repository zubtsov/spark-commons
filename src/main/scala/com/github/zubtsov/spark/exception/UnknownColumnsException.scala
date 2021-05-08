package com.github.zubtsov.spark.exception

final case class UnknownColumnsException(unknownColumns: Seq[String]) extends Exception {
  override def getMessage: String = s"Unknown columns specified: ${unknownColumns.mkString(", ")}"
}
