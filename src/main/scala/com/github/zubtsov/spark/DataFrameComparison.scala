package com.github.zubtsov.spark

import com.github.zubtsov.spark.enums.JoinType
import com.github.zubtsov.spark.exception.{DifferentColumnNamesException, DifferentDataException, DifferentSchemasException}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

//FIXME: how to deal with duplicates in approximate methods? group by all columns and count rows?
//TODO: support case insensitive string comparison
//TODO: add getDataDifferenceIgnoreDuplicates with the list of columns
//TODO: add join stats calculator to check how many rows were joined/not joined/duplicated

/**
 * Utility object to compare [[org.apache.spark.sql.DataFrame]]s' columns, schemas and data
 */
object DataFrameComparison {
  final case class ColumnNamesDifference(leftMissingCols: Seq[String], rightMissingCols: Seq[String]) {
    lazy val isDifferent: Boolean = leftMissingCols.nonEmpty || rightMissingCols.nonEmpty

    def isTheSame: Boolean = !isDifferent

    override def toString: String = s"Missing columns from the left: ${leftMissingCols.mkString(", ")}" +
      s"\nMissing columns from the right: ${rightMissingCols.mkString(", ")}"
  }

  final case class SchemaDifference(leftMissingFields: Seq[StructField], rightMissingFields: Seq[StructField]) {
    lazy val isDifferent: Boolean = leftMissingFields.nonEmpty || rightMissingFields.nonEmpty

    def isTheSame: Boolean = !isDifferent

    override def toString: String = s"Missing columns from the left: ${StructType(leftMissingFields).toDDL}" +
      s"\nMissing columns from the right: ${StructType(rightMissingFields).toDDL}"
  }

  final case class DataDifference(leftMissingRows: DataFrame, rightMissingRows: DataFrame) { //TODO: case class probably doesn't make much sense here
    lazy val isDifferent: Boolean = !leftMissingRows.isEmpty || !rightMissingRows.isEmpty

    def isTheSame: Boolean = !isDifferent

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

  def getDataDifferenceApproximate2(left: DataFrame, right: DataFrame, primaryKey: Seq[String], numericColToAbsError: Map[String, Column]): DataDifference = {
    val joinCols = primaryKey
    val nonNumericColsEqual = joinCols.map(cn => left(cn) === right(cn)).reduce(_ and _)
    val numericColsAreWithinApproxError = numericColToAbsError.map(t => abs(left(t._1) - right(t._1)) <= t._2).reduce(_ and _)
    val joinCondition = nonNumericColsEqual and numericColsAreWithinApproxError
    val leftMissingRows = right.join(left, joinCondition, JoinType.LeftAnti)
    val rightMissingRows = left.join(right, joinCondition, JoinType.LeftAnti)
    DataDifference(leftMissingRows, rightMissingRows)
  }

  def getDataDifferenceApproximate(left: DataFrame, right: DataFrame, primaryKey: Seq[String], numericColToAbsError: Map[String, Double]): DataDifference = {
    getDataDifferenceApproximate2(left, right, primaryKey, numericColToAbsError.mapValues(v => lit(v)))
  }

  def getDataDifferenceApproximate(left: DataFrame, right: DataFrame, numericColToAbsError: Map[String, Double]): DataDifference = {
    getDataDifferenceApproximate(left, right, left.drop(numericColToAbsError.keys.toSeq:_*).columns, numericColToAbsError)
  }

  def getDataDifferenceApproximate(left: DataFrame, right: DataFrame, numericCols: Seq[String], absoluteError: Double): DataDifference = {
    getDataDifferenceApproximate(left, right, numericCols.map(cn => (cn, absoluteError)).toMap)
  }

  def hasEqualColumnNames(leftColumns: Seq[String], rightColumns: Seq[String], caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getColumnNamesDifference(leftColumns, rightColumns, caseSensitive).isTheSame
  }

  def hasDifferentColumnNames(leftColumns: Seq[String], rightColumns: Seq[String], caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getColumnNamesDifference(leftColumns, rightColumns, caseSensitive).isDifferent
  }

  def hasEqualSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive).isTheSame
  }

  def hasDifferentSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Boolean = {
    getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive).isDifferent
  }

  def hasEqualData(left: DataFrame, right: DataFrame): Boolean = {
    getDataDifference(left, right).isTheSame
  }

  def hasDifferentData(left: DataFrame, right: DataFrame): Boolean = {
    getDataDifference(left, right).isDifferent
  }

  def assertEqualColumnNames(left: DataFrame, right: DataFrame, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    val columnNamesDifference = getColumnNamesDifference(left.columns, right.columns, caseSensitive)
    if (columnNamesDifference.isDifferent) {
      throw DifferentColumnNamesException(columnNamesDifference)
    }
  }

  def assertEqualSchemas(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    val schemaDifference = getSchemaDifference(left.schema, right.schema, ignoreNullability, caseSensitive)
    if (schemaDifference.isDifferent) {
      throw DifferentSchemasException(schemaDifference)
    }
  }

  def assertEqualData(left: DataFrame, right: DataFrame): Unit = {
    val dataDifference = getDataDifference(left, right)
    if (dataDifference.isDifferent) {
      throw DifferentDataException(dataDifference)
    }
  }

  def assertEquals(left: DataFrame, right: DataFrame, ignoreNullability: Boolean = true, caseSensitive: Boolean = defaultCaseSensitivity): Unit = {
    assertEqualColumnNames(left, right, caseSensitive)
    assertEqualSchemas(left, right, ignoreNullability, caseSensitive)
    assertEqualData(left, right)
  }
}
