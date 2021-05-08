package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.enums.ColumnPosition
import com.github.zubtsov.spark.enums.ColumnPosition.ColumnPosition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Utility class for the proper ID generation.
 * [[org.apache.spark.sql.functions.row_number()]] is bad from the performance stand point (all data has to be shuffled into one partition).
 * [[org.apache.spark.sql.functions.monotonically_increasing_id()]] has large gaps and will rapidly overflow in case of multiple incremental id generations.
 * Generating UID [[java.util.UUID]] or calculating some hash of a row can also be a solution, but there's a possibility of collisions.
 */
object DataFrameWithId {

  object implicits {
    implicit class DataFrameWithIdImpl(df: DataFrame) {
      def zipWithIndex(offset: Long = 1L,
                       idColumnName: String = "id",
                       pos: ColumnPosition = ColumnPosition.Tail): DataFrame = {
        df.transform(DataFrameWithId.zipWithIndex(offset, idColumnName, pos))
      }

      def zipWithUniqueId(offset: Long = 1L,
                          idColumnName: String = "id",
                          pos: ColumnPosition = ColumnPosition.Tail): DataFrame = {
        df.transform(zipWith(_.zipWithUniqueId())(offset, idColumnName, pos))
      }
    }
  }

  /**
   * @param offset the starting value of the identifier (included)
   * @param idColumnName the name of the identifier column
   * @param pos the position of a column relative to the existing columns
   * @param df a table to which the identifier will be added
   * @return data frame with continuous unique identifier column
   */
  def zipWithIndex(offset: Long = 1L,
                   idColumnName: String = "id",
                   pos: ColumnPosition = ColumnPosition.Tail)
                  (df: DataFrame): DataFrame = {
    zipWith(_.zipWithIndex())(offset, idColumnName, pos)(df)
  }

  /**
   * @param offset the starting value of the identifier (included)
   * @param idColumnName the name of the identifier column
   * @param pos the position of a column relative to the existing columns
   * @param df a table to which the identifier will be added
   * @return data frame with unique identifier column with possible discontinuities (gaps)
   */
  def zipWithUniqueId(offset: Long = 1L,
                      idColumnName: String = "id",
                      pos: ColumnPosition = ColumnPosition.Tail)
                     (df: DataFrame): DataFrame = {
    zipWith(_.zipWithUniqueId())(offset, idColumnName, pos)(df)
  }

  private def zipWith(rddIdFunc: RDD[Row] => RDD[(Row, Long)])(offset: Long, idColumnName: String, pos: ColumnPosition)
                     (df: DataFrame): DataFrame = {
    val (schema, rdd) = pos match {
      case ColumnPosition.Head => {
        (
          StructType(StructField(idColumnName, LongType) +: df.schema.fields),
          rddIdFunc(df.rdd).map(tp => Row.fromSeq((tp._2 + offset) +: tp._1.toSeq))
        )
      }
      case ColumnPosition.Tail => {
        (
          StructType(df.schema.fields :+ StructField(idColumnName, LongType)),
          rddIdFunc(df.rdd).map(tp => Row.fromSeq(tp._1.toSeq :+ (tp._2 + offset)))
        )
      }
    }

    SparkSession.active.createDataFrame(rdd, schema)
  }
}
