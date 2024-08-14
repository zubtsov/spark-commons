package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.enums.ColumnPosition.ColumnPosition
import com.github.zubtsov.spark.enums.UnionStrategy.UnionStrategy
import com.github.zubtsov.spark.enums.{ColumnPosition, UnionStrategy}
import com.github.zubtsov.spark.exception.ColumnAlreadyExistsException
import com.github.zubtsov.spark.{areStringsEqual, defaultCaseSensitivity}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Miscellaneous unclassified functions
 */
object DataFrameCommons {

  object implicits {

    implicit class DataFrameCommonsImpl(df: DataFrame) {
      /**
       * A more readable and meaningful (comparing to the exclamation sign) method for non-emptiness check
       * @return
       */
      def isNotEmpty(): Boolean = {
        !df.isEmpty
      }

      /**
       * A more readable and meaningful method (comparing to the exclamation sign) for non-emptiness check
       * @return
       */
      def nonEmpty(): Boolean = {
        !df.isEmpty
      }

      /**
       * A counterpart of the [[DataFrame]]'s sample() method
       * @return
       */
      def sample2(withReplacement: Boolean, numRows: Int, seed: Long): DataFrame = {
        df.sample(withReplacement, calculateFraction(numRows), seed).limit(numRows)
      }

      /**
       * A counterpart of the [[DataFrame]]'s sample() method
       * @return
       */
      def sample2(numRows: Int, seed: Long): DataFrame = {
        df.sample(calculateFraction(numRows), seed).limit(numRows)
      }

      /**
       * A counterpart of the [[DataFrame]]'s sample() method
       * @return
       */
      def sample2(withReplacement: Boolean, numRows: Int): DataFrame = {
        df.sample(withReplacement, calculateFraction(numRows)).limit(numRows)
      }

      /**
       * A counterpart of the [[DataFrame]]'s sample() method
       * @return
       */
      def sample2(numRows: Int): DataFrame = {
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
                       pos: ColumnPosition = ColumnPosition.Last
                     ): DataFrame = {
        val colExists = df.columns.exists(cn => areStringsEqual(false)(cn, colName)) //withColumn is case insensitive

        if (colExists && !replaceIfExists) {
          throw ColumnAlreadyExistsException(colName)
        }

        if (pos == ColumnPosition.Last) {
          df.withColumn(colName, colValue)
        } else {
          df.withColumn(colName, colValue)
            .select(colName, df.columns: _*)
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
                 unionStrategy: UnionStrategy = UnionStrategy.AllColumns): DataFrame = {
        val left = df
        lazy val commonColumns = left.columns.filter(cn => right.columns.exists(cn2 => areStringsEqual(false)(cn, cn2)))
        lazy val leftOnlyColumns = left.columns.filter(cn => !right.columns.exists(cn2 => areStringsEqual(false)(cn, cn2)))
        lazy val rightOnlyColumns = right.columns.filter(cn => !left.columns.exists(cn2 => areStringsEqual(false)(cn, cn2)))
        lazy val leftWithEmptyRightCols = left.select((leftOnlyColumns ++ commonColumns).map(col) ++ rightOnlyColumns.map(lit(null).as(_)): _*)
        lazy val rightWithEmptyLeftCols = right.select((rightOnlyColumns ++ commonColumns).map(col) ++ leftOnlyColumns.map(lit(null).as(_)): _*)
        lazy val leftCommonWithEmptyRightCols = left.select(commonColumns.map(col) ++ rightOnlyColumns.map(lit(null).as(_)): _*)
        lazy val rightCommonWithEmptyLeftCols = right.select(commonColumns.map(col) ++ leftOnlyColumns.map(lit(null).as(_)): _*)
        unionStrategy match {
          case UnionStrategy.AllColumns => leftWithEmptyRightCols unionByName rightWithEmptyLeftCols
          case UnionStrategy.CommonColumns => left.select(commonColumns.map(col): _*) unionByName right.select(commonColumns.map(col): _*)
          case UnionStrategy.LeftColumns => left unionByName rightCommonWithEmptyLeftCols
          case UnionStrategy.RightColumns => right unionByName leftCommonWithEmptyRightCols
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
        //TODO: should we throw an exception when column is not found or quietly skip it or remove the case sensitivity parameter?
        val columnsWithDuplicates = df.schema.filter(t => columns.exists(cn => areStringsEqual(caseSensitive)(t.name, cn)))

        val tmpColNameMapper: StructField => String = sf => sf.name + sf.name.hashCode
        val (stringColsWithDuplicates, otherColsWithDuplicates) = columnsWithDuplicates.partition(sf => sf.dataType == StringType)

        val tmpStringColNames = stringColsWithDuplicates.map(tmpColNameMapper)

        val oldColToTmpColName = stringColsWithDuplicates.zip(tmpStringColNames)
        val colsForDroppingDuplicates = tmpStringColNames ++ otherColsWithDuplicates.map(_.name)

        val dfWithTemporaryCols = oldColToTmpColName.foldLeft(df)((df, t) => {
          df.withColumn(t._2, lower(col(t._1.name)))
        })

        dfWithTemporaryCols
          .dropDuplicates(colsForDroppingDuplicates)
          .drop(tmpStringColNames: _*)
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
      def unpivot2(columns: Seq[String],
                  nameColumn: String = "name",
                  valueColumn: String = "value",
                  caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val unpivotedColFilter = (cn: String) => columns.exists(areStringsEqual(caseSensitive)(cn, _))
        val notUnpivotedColFilter = (cn: String) => !unpivotedColFilter(cn)

        val remainingCols = df.columns.filter(notUnpivotedColFilter).map(col)
        val unpivotExpr = explode(map(df.columns.filter(unpivotedColFilter).flatMap(c => Seq(lit(c), col(c))):_*)).as(Seq(nameColumn, valueColumn))
        df.select(remainingCols :+ unpivotExpr: _*)
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
      def unpivot3(columns: Seq[String],
                  nameColumn: String = "name",
                  valueColumn: String = "value",
                  caseSensitive: Boolean = defaultCaseSensitivity): DataFrame = {
        val unpivotedColFilter = (cn: String) => columns.exists(areStringsEqual(caseSensitive)(cn, _))
        val notUnpivotedColFilter = (cn: String) => !columns.exists(areStringsEqual(caseSensitive)(cn, _))

        val remainingColumns = df.columns.filter(notUnpivotedColFilter).map(col)
        val unpivotedColumns = df.columns.filter(unpivotedColFilter)

        val unpivotedColsExpr = unpivotedColumns.map(cn => s"'${cn}', ${cn}").mkString(", ")
        val unpivotExpr = expr(s"stack(${unpivotedColumns.length}, ${unpivotedColsExpr}) as (${nameColumn}, ${valueColumn})")
        df.select(remainingColumns :+ unpivotExpr: _*)
      }

      //TODO: implement transpose function (like matrix transposition)

      private def calculateFraction(numRows: Int): Double = {
        val tableSize = df.count().asInstanceOf[Double]
        val extraRows = (numRows * 0.1 + 10).toInt //unfortunately, it's unclear how (or even not possible) to guarantee numRows size
        if (numRows < tableSize - extraRows) {
          val fraction = (numRows + extraRows) / tableSize
          fraction
        } else {
          1
        }
      }
    }
  }
}
