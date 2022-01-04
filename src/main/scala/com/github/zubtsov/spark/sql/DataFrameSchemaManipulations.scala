package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.exception.UnknownColumnsException
import com.github.zubtsov.spark.{areStringsEqual, defaultCaseSensitivity}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object DataFrameSchemaManipulations {
  private val unicodeWhitespaceRegex = "^[\\p{Z}\\p{C}]+|[\\p{Z}\\p{C}]+$"

  object implicits {
    implicit class DataFrameSchemaManipulationsImpl(df: DataFrame) {
      /**
       * When reading CSV files there might be some whitespaces in column names, this method helps to remove them
       * @param trimmedColumnNames list of column names to be trimmed (enclosing whitespaces are not taken into account)
       * @param caseSensitive defines whether to compare column names in case sensitive way
       * @return table with trimmed column names
       */
      def withTrimmedColumnNames(trimmedColumnNames: Seq[String] = df.columns, caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val trimFunction = (s: String) => s.trim
        withTrimmedColNames(trimmedColumnNames, caseSensitive, trimFunction)
      }
      /**
       * When reading CSV files there might be some whitespaces (including Unicode) in column names, this method helps to remove them
       * @param trimmedColumnNames list of column names to be trimmed (enclosing whitespaces are not taken into account)
       * @param caseSensitive defines whether to compare column names in case sensitive way
       * @return table with trimmed column names
       */
      def withUnicodeTrimmedColumnNames(trimmedColumnNames: Seq[String] = df.columns, caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val trimFunction = (s: String) => s.replaceAll(unicodeWhitespaceRegex, "")
        withTrimmedColNames(trimmedColumnNames, caseSensitive, trimFunction)
      }

      //TODO: get rid of boolean parameters, replace them with a case class
      /**
       * Allows to cast the entire schema of the table to the target schema
       * @param targetSchema
       * @param addMissingColumns
       * @param removeExcessiveColumns
       * @param caseSensitive defines whether to compare column names in case sensitive way
       * @return the table with the target schema
       */
      def cast(targetSchema: StructType,
               addMissingColumns: Boolean = true,
               removeExcessiveColumns: Boolean = true,
               caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val sc: (String, String) => Boolean = areStringsEqual(caseSensitive)
        val targetColumns = targetSchema.map(_.name)

        val excessiveColumnsToSelect = df.schema
          .filter(sf => !targetColumns.exists(cn2 => sc(sf.name, cn2)))
          .map(sf => col(sf.name).cast(sf.dataType))

        val commonAndMissingColsToSelect = targetSchema
          .filter(sf => df.columns.exists(cn => sc(sf.name, cn)) || addMissingColumns)
          .map(sf => col(sf.name).cast(sf.dataType))

        val resultColumnsToSelect = commonAndMissingColsToSelect ++ (if (removeExcessiveColumns) Seq.empty
        else excessiveColumnsToSelect)

        df.select(resultColumnsToSelect: _*)
      }

      /**
       * The function allows to sort table columns in the specified order
       * @param f
       * @param ord
       * @tparam B
       * @return
       */
      def sortColumns[B](f: String => B)(implicit ord: Ordering[B]): DataFrame = {
        df.select(df.columns.sortBy(f)(ord).map(col): _*)
      }

      private def assertAllColumnsExist(columns: Seq[String], caseSensitive: Boolean, trimFunction: String => String) = {
        val unknownColumns = columns.filter(cn => !df.columns.exists(cn2 => areStringsEqual(caseSensitive)(trimFunction(cn), trimFunction(cn2))))
        if (unknownColumns.nonEmpty) {
          throw UnknownColumnsException(unknownColumns)
        }
      }

      private def withTrimmedColNames(columns: Seq[String] = df.columns, caseSensitive: Boolean, trimFunction: String => String): DataFrame = {
        assertAllColumnsExist(columns, caseSensitive, trimFunction)

        df.select(
          df.columns
            .map(cn => {
              if (columns.exists(cn2 => areStringsEqual(caseSensitive)(trimFunction(cn), trimFunction(cn2)))) {
                col(cn).as(trimFunction(cn))
              } else {
                col(cn)
              }
            }).toSeq: _*
        )
      }
    }
  }

}
