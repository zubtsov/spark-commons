package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.zubtsov.{areStringsEqual, defaultCaseSensitivity}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * typical column operations performed on multiple columns at once
 */
object DataFrameBulkColumnOperations {

  implicit class DataFrameBulkColumnOperations(df: DataFrame) {

    import com.github.zubtsov.spark.sql.DataFrameColumns.implicits._

    def trimStrings(colNames: Seq[String] = df.stringColumns, caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
      transformStrings(colNames, caseSensitive, cn => trim(col(cn)))
    }

    def cutStrings(colNames: Seq[String],
                   maximumLength: Int,
                   caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
      transformStrings(colNames, caseSensitive, cn => substring(col(cn), 0, maximumLength))
    }

    def cutStrings2(colNameToMaxLength: Map[String, Int], caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
      val loweredColNameToMaxLength = if (caseSensitive) colNameToMaxLength else colNameToMaxLength.map(t => t.copy(t._1.toLowerCase))
      val f = (cn: String) => {
        val colName = if (caseSensitive) cn else cn.toLowerCase
        substring(col(cn), 0, loweredColNameToMaxLength(colName))
      }
      transformStrings(colNameToMaxLength.keys.toSeq, caseSensitive, f)
    }

    def regexpReplace(pattern: String,
                      replacement: String,
                      colNames: Seq[String],
                      caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
      transformStrings(colNames, caseSensitive, cn => regexp_replace(col(cn), pattern, replacement))
    }

    private def transformStrings(colNames: Seq[String] = df.stringColumns, caseSensitive: Boolean = defaultCaseSensitivity, f: String => Column) = {
      val sc: (String, String) => Boolean = areStringsEqual(caseSensitive)
      val colsToSelect = df.schema.map(sf => sf.dataType match {
        case StringType if colNames.exists(cn => sc(cn, sf.name)) => f(sf.name)
        case _ => col(sf.name)
      })
      df.select(colsToSelect: _*)
    }
  }
}
