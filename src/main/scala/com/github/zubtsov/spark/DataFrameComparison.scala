package com.github.zubtsov.spark

import com.github.zubtsov.spark.exception.{DifferentColumnNamesException, DifferentDataException, DifferentSchemasException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

//TODO: add approximate comparison of double values with some epsilon & case insensitive string comparison

/**
 * Utility object to compare [[org.apache.spark.sql.DataFrame]]s' columns, schemas and data
 */
object DataFrameComparison {
  final case class ColumnNamesDifference(leftMissingCols: Seq[String], rightMissingCols: Seq[String]) {
    def isDifferent(): Boolean = leftMissingCols.nonEmpty || rightMissingCols.nonEmpty

    def isTheSame(): Boolean = !isDifferent()

    override def toString: String = s"Missing columns from the left: ${leftMissingCols.mkString(", ")}" +
      s"\nMissing columns from the right: ${rightMissingCols.mkString(", ")}"
  }

  final case class SchemaDifference(leftMissingFields: Seq[StructField], rightMissingFields: Seq[StructField]) {
    def isDifferent(): Boolean = leftMissingFields.nonEmpty || rightMissingFields.nonEmpty

    def isTheSame(): Boolean = !isDifferent()

    override def toString: String = s"Missing columns from the left: ${StructType(leftMissingFields).toDDL}" +
      s"\nMissing columns from the right: ${StructType(rightMissingFields).toDDL}"
  }

  final case class DataDifference(leftMissingRows: DataFrame, rightMissingRows: DataFrame) { //TODO: case class probably doesn't make much sense here
    def isDifferent(): Boolean = !leftMissingRows.isEmpty || !rightMissingRows.isEmpty

    def isTheSame(): Boolean = !isDifferent()

    override def toString: String = s"The number of missing rows from the left: ${leftMissingRows.count()}" +
      s"\nThe number of missing rows from the right: ${rightMissingRows.count()}"
  }

  def getColumnNamesDifference(leftColumns: Seq[String], rightColumns: Seq[String], caseSensitive: Boolean = defaultCaseSensitivity): ColumnNamesDifference = {
    val sc = areStringsEqual(caseSensitive)(_, _)
    val leftMissingCols = rightColumns.filter(lcn => !leftColumns.exists(rcn => sc(lcn, rcn)))
    val rightMissingCols = leftColumns.filter(lcn => !rightColumns.exists(rcn => sc(lcn, rcn)))
    ColumnNamesDifference(leftMissingCols, rightMissingCols)
  }

  def getSchemaDifference(left: StructType, right: StructType, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): SchemaDifference = {
    val sc = areStringsEqual(caseSensitive)(_, _)
    val nc = (sf1: StructField, sf2: StructField) => if (ignoreNullability) true else sf1.nullable == sf2.nullable
    val tc = (sf1: StructField, sf2: StructField) => sf1.dataType == sf2.dataType
    val leftMissingFields = right.filter(lsf => !left.exists(rsf => sc(lsf.name, rsf.name) && nc(lsf, rsf) && tc(lsf, rsf)))
    val rightMissingFields = left.filter(lsf => !right.exists(rsf => sc(lsf.name, rsf.name) && nc(lsf, rsf) && tc(lsf, rsf)))
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
  //FIXME: how to deal with duplicates? count rows? add signature with primary key? add signature with error per column (use map)
  def getDataDifferenceApproximate(left: DataFrame, right: DataFrame, numericCols: Seq[String], absoluteError: Double): DataDifference = {
    val otherCols = left.drop(numericCols:_*).columns
    val nonNumericColsEqual = otherCols.map(cn => left(cn) === right(cn)).reduce(_ and _)
    val numericColsAreWithinApproxError = numericCols.map(cn => abs(left(cn) - right(cn)) < absoluteError).reduce(_ and _)
    val joinCondition = nonNumericColsEqual and numericColsAreWithinApproxError
    val leftMissingRows = right.join(left, joinCondition, "left_anti")
    val rightMissingRows = left.join(right, joinCondition, "left_anti")
    DataDifference(leftMissingRows, rightMissingRows)
  }

  def hasEqualColumnNames(leftColumns: Seq[String], rightColumns: Seq[String], caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getColumnNamesDifference(leftColumns, rightColumns, caseSensitive).isTheSame()
  }

  def hasDifferentColumnNames(leftColumns: Seq[String], rightColumns: Seq[String], caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getColumnNamesDifference(leftColumns, rightColumns, caseSensitive).isDifferent()
  }

  def hasEqualSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive).isTheSame()
  }

  def hasDifferentSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive).isDifferent()
  }

  def hasEqualData(left: DataFrame, right: DataFrame): Boolean = {
    getDataDifference(left, right).isTheSame()
  }

  def hasDifferentData(left: DataFrame, right: DataFrame): Boolean = {
    getDataDifference(left, right).isDifferent()
  }

  def assertEqualColumnNames(left: DataFrame, right: DataFrame, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    val columnNamesDifference = getColumnNamesDifference(left.columns, right.columns, caseSensitive)
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
