package com.github.zubtsov.spark

import com.github.zubtsov.spark.exception.{DifferentColumnNamesException, DifferentDataException, DifferentSchemasException}
import com.github.zubtsov.spark.zubtsov.{areStringsEqual, defaultCaseSensitivity}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Utility object to compare [[org.apache.spark.sql.DataFrame]]s' columns, schemas and data
 */
object DataFrameComparison {
  final case class ColumnNamesDifference(leftMissingCols: Seq[String], rightMissingCols: Seq[String]) {
    def isDifferent(): Boolean = leftMissingCols.nonEmpty || rightMissingCols.nonEmpty

    def isTheSame(): Boolean = !isDifferent()
  }

  final case class SchemaDifference(leftMissingFields: Seq[StructField], rightMissingFields: Seq[StructField]) {
    def isDifferent(): Boolean = leftMissingFields.nonEmpty || rightMissingFields.nonEmpty

    def isTheSame(): Boolean = !isDifferent()
  }

  final case class DataDifference(leftMissingRows: DataFrame, rightMissingRows: DataFrame) { //TODO: case class probably doesn't make much sense here
    def isDifferent(): Boolean = !leftMissingRows.isEmpty || !rightMissingRows.isEmpty

    def isTheSame(): Boolean = !isDifferent()
  }

  def getColumnNamesDifference(left: DataFrame, right: DataFrame, caseSensitive: Boolean = defaultCaseSensitivity): ColumnNamesDifference = {
    val sc = areStringsEqual(caseSensitive)(_, _)
    val leftMissingCols = right.columns.filter(lcn => !left.columns.exists(rcn => sc(lcn, rcn)))
    val rightMissingCols = left.columns.filter(lcn => !right.columns.exists(rcn => sc(lcn, rcn)))
    ColumnNamesDifference(leftMissingCols, rightMissingCols)
  }

  def getSchemaDifference(left: StructType, right: StructType, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): SchemaDifference = {
    val sc = areStringsEqual(caseSensitive)(_, _)
    val nc = (sf1: StructField, sf2: StructField) => if (ignoreNullability) true else sf1.nullable == sf2.nullable
    val leftMissingFields = right.filter(lsf => !left.exists(rsf => sc(lsf.name, rsf.name) && nc(lsf, rsf)))
    val rightMissingFields = left.filter(lsf => !right.exists(rsf => sc(lsf.name, rsf.name) && nc(lsf, rsf)))
    SchemaDifference(leftMissingFields, rightMissingFields)
  }

  def getDataDifference(left: DataFrame, right: DataFrame): DataDifference = {
    val cols = left.columns.map(col)
    val leftProjected = left.select(cols: _*)
    val rightProjected = right.select(cols: _*)
    val leftMissingRows = rightProjected.exceptAll(leftProjected)
    val rightMissingRows = leftProjected.exceptAll(rightProjected)
    DataDifference(leftMissingRows, rightMissingRows)
  }

  def hasEqualColumnNames(left: DataFrame, right: DataFrame, caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getColumnNamesDifference(left, right, caseSensitive).isTheSame()
  }

  def hasEqualSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive).isTheSame()
  }

  def hasEqualData(left: DataFrame, right: DataFrame): Unit = {
    getDataDifference(left, right).isTheSame()
  }

  def assertEqualColumnNames(left: DataFrame, right: DataFrame, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    val columnNamesDifference = getColumnNamesDifference(left, right, caseSensitive)
    if (columnNamesDifference.isDifferent()) {
      throw DifferentColumnNamesException(columnNamesDifference)
    }
  }

  def assertEqualSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    val schemaDifference = getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive)
    if (schemaDifference.isDifferent()) {
      throw DifferentSchemasException(schemaDifference)
    }
  }

  def assertEqualData(left: DataFrame, right: DataFrame): Unit = {
    val dataDifference = getDataDifference(left, right)
    if (dataDifference.isDifferent()) {
      throw DifferentDataException(dataDifference)
    }
  }

  def assertEquals(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    assertEqualColumnNames(left, right, caseSensitive)
    assertEqualSchemas(left, right, ignoreNullability, caseSensitive)
    assertEqualData(left, right)
  }
}
