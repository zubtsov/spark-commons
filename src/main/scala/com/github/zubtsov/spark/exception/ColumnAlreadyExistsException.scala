package com.github.zubtsov.spark.exception

final case class ColumnAlreadyExistsException(columnName: String) extends Exception {
  override def getMessage: String = s"A column with name `${columnName}` already exists"
}
