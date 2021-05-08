package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.enums.ColumnPosition.ColumnPosition
import com.github.zubtsov.spark.enums.UnionStrategy.UnionStrategy
import com.github.zubtsov.spark.enums.{ColumnPosition, UnionStrategy}
import com.github.zubtsov.spark.zubtsov.{areStringsEqual, defaultCaseSensitivity}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Miscellaneous unclassified functions
 */
object DataFrameCommons {

  object implicits {
    implicit class DataFrameCommonsImpl(df: DataFrame) {
      def isNotEmpty(): Boolean = {
        !df.isEmpty
      }

      def nonEmpty(): Boolean = {
        !df.isEmpty
      }

      def sample(withReplacement: Boolean, numRows: Int, seed: Long): DataFrame = {
        df.sample(withReplacement, calculateFraction(numRows), seed).limit(numRows)
      }

      def sample(numRows: Int, seed: Long): DataFrame = {
        df.sample(calculateFraction(numRows), seed).limit(numRows)
      }

      def sample(withReplacement: Boolean, numRows: Int): DataFrame = {
        df.sample(withReplacement, calculateFraction(numRows)).limit(numRows)
      }

      def sample(numRows: Int): DataFrame = {
        df.sample(calculateFraction(numRows)).limit(numRows)
      }

      /**
       * More flexible version of [[org.apache.spark.sql.Dataset.withColumn]]
       *
       * @param colName
       * @param colValue
       * @param replaceIfExists
       * @param pos
       * @param caseSensitive
       * @return
       */
      def withColumn2(
                       colName: String,
                       colValue: Column,
                       replaceIfExists: Boolean = false,
                       pos: ColumnPosition = ColumnPosition.Tail,
                       caseSensitive: Boolean = defaultCaseSensitivity
                     ): DataFrame = {
        val colExists = df.columns.exists(cn => areStringsEqual(caseSensitive)(cn, colName))
        if (colExists && replaceIfExists) {
          if (pos == ColumnPosition.Tail) {
            df.withColumn(colName, colValue)
          } else {
            df.withColumn(colName, colValue)
              .select(colName, df.columns: _*)
          }
        } else {
          df
        }
      }

      /**
       * More flexible version of the union operation
       *
       * @param right
       * @param unionStrategy defines the resulting schema. See [[UnionStrategy]]
       * @param caseSensitive
       * @return united table
       */
      def union2(right: DataFrame,
                 unionStrategy: UnionStrategy = UnionStrategy.AllColumns,
                 caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val left = df
        val commonColumns = left.columns.filter(cn => right.columns.exists(cn2 => areStringsEqual(caseSensitive)(cn, cn2)))
        val leftOnlyColumns = left.columns.filter(cn => !right.columns.exists(cn2 => areStringsEqual(caseSensitive)(cn, cn2)))
        val rightOnlyColumns = right.columns.filter(cn => !left.columns.exists(cn2 => areStringsEqual(caseSensitive)(cn, cn2)))
        val leftWithEmptyRightCols = left.select(commonColumns.map(col) ++ rightOnlyColumns.map(lit(null).as(_)): _*)
        val rightWithEmptyLeftCols = right.select(commonColumns.map(col) ++ leftOnlyColumns.map(lit(null).as(_)): _*)
        unionStrategy match {
          case UnionStrategy.AllColumns => leftWithEmptyRightCols unionByName rightWithEmptyLeftCols
          case UnionStrategy.CommonColumns => left.select(commonColumns.map(col): _*) unionByName right.select(commonColumns.map(col): _*)
          case UnionStrategy.LeftColumns => left unionByName rightWithEmptyLeftCols
          case UnionStrategy.RightColumns => right unionByName leftWithEmptyRightCols
        }
      }

      /**
       * Drops duplicates ignoring values case
       *
       * @param columns
       * @param caseSensitive
       * @return
       */
      def dropDuplicatesIgnoreCase(columns: Seq[String] = df.columns,
                                   caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val colNamesProperCase = columns.map(cn1 => df.columns.find(cn2 => areStringsEqual(caseSensitive)(cn1, cn2)).get)
        val mapFunc: String => String = cn => cn + cn.hashCode
        val tmpColNames = colNamesProperCase.map(mapFunc)

        colNamesProperCase.zip(tmpColNames).foldLeft(df)((df, t) => {
          df.withColumn(t._2, lower(col(t._1))) //TODO: avoid doing it for non-string columns, use stringColumns function
        }).dropDuplicates(tmpColNames)
          .drop(tmpColNames: _*)
      }

      /**
       * Turns specified columns into rows
       *
       * @param columns
       * @param nameColumn
       * @param valueColumn
       * @param caseSensitive
       * @return
       */
      def unpivot(columns: Seq[String],
                  nameColumn: String = "name",
                  valueColumn: String = "value",
                  caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val notUnpivotedCol = (cn: String) => !columns.exists(areStringsEqual(caseSensitive)(cn, _))

        val remainingCols = df.columns.filter(notUnpivotedCol).map(col)
        val unpivotedColsExpr = columns.map(cn => s"'${cn}', ${cn}").mkString(", ")
        val unpivotExpr = expr(s"stack(${columns.length}, ${unpivotedColsExpr}) as (${nameColumn}, ${valueColumn})")
        df.select(remainingCols :+ unpivotExpr: _*)
      }

      //TODO: implement transpose function (like matrix transposition)

      private def calculateFraction(numRows: Int): Double = {
        val fraction = df.count().asInstanceOf[Double] / (numRows + 1)
        fraction
      }
    }
  }
}
