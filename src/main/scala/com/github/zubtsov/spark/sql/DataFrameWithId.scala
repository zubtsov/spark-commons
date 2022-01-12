package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.enums.ColumnPosition
import com.github.zubtsov.spark.enums.ColumnPosition.ColumnPosition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
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
      /**
       * [[DataFrameWithId.zipWithId()]]
       */
      def zipWithId(offset: Long = 1L, idColumnName: String = "id", pos: ColumnPosition = ColumnPosition.Last): DataFrame = {
        df.transform(DataFrameWithId.zipWithId(offset, idColumnName, pos))
      }
      /**
       * [[DataFrameWithId.zipWithIndex()]]
       */
      def zipWithIndex(offset: Long = 1L,
                       idColumnName: String = "id",
                       pos: ColumnPosition = ColumnPosition.Last): DataFrame = {
        df.transform(DataFrameWithId.zipWithIndex(offset, idColumnName, pos))
      }
      /**
       * [[DataFrameWithId.zipWithUniqueId()]]
       */
      def zipWithUniqueId(offset: Long = 1L,
                          idColumnName: String = "id",
                          pos: ColumnPosition = ColumnPosition.Last): DataFrame = {
        df.transform(zipWith(_.zipWithUniqueId())(offset, idColumnName, pos))
      }
    }
  }

  /**
   * This implementation is "RDD-free", but uses UDF. Inspired by: https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/48454000#48454000
   * @param offset the starting value of the identifier (included)
   * @param idColumnName the name of the identifier column
   * @param pos the position of a column relative to the existing columns
   * @param df a table to which the identifier will be added
   * @return data frame with continuous unique identifier column
   */
  def zipWithId(offset: Long = 1, idColumnName: String = "id", pos: ColumnPosition = ColumnPosition.Last)(df: DataFrame): DataFrame = {
    val partitionIdColName = "_tmp_partition_id_"
    val monotonicallyIncreasingIdColName = "_tmp_monotonically_increasing_id_"

    val numberOfRecordsPerPartitionColName = "_tmp_number_of_records_per_partition_"
    val partitionOffsetColName = "_tmp_partition_offset_"

    val dfWithPartitionId = df
      .withColumn(partitionIdColName, spark_partition_id().cast(LongType))
      .withColumn(monotonicallyIncreasingIdColName, monotonically_increasing_id())

    val partitionsOffsets: Map[Long, Long] = dfWithPartitionId
      .groupBy(partitionIdColName)
      .agg(
        count(lit(1)).as(numberOfRecordsPerPartitionColName),
        min(monotonicallyIncreasingIdColName).as(monotonicallyIncreasingIdColName) //try to use min(...) in case first(...) doesn't work
      )
      .select(
        col(partitionIdColName),
        (
          sum(numberOfRecordsPerPartitionColName)
            .over(Window.orderBy(partitionIdColName)).as("total_number_of_records") -
          col(numberOfRecordsPerPartitionColName) -
          col(monotonicallyIncreasingIdColName) +
          lit(offset)
        ).as(partitionOffsetColName)
      )
      .collect()
      .map(row => (row.getAs[Long](partitionIdColName), row.getAs[Long](partitionOffsetColName))).toMap

    val idColumn = (col(partitionOffsetColName) + col(monotonicallyIncreasingIdColName)).as(idColumnName)

    val colsToSelect = pos match {
      case ColumnPosition.First => Seq(idColumn) ++ dfWithPartitionId.columns.map(col)
      case ColumnPosition.Last => dfWithPartitionId.columns.map(col).toSeq ++ Seq(idColumn)
    }

    val mapPartitionIdToOffset = udf((partitionId: Int) => partitionsOffsets(partitionId)) //unfortunately, DataFrame.na.replace doesn't support Long type

    dfWithPartitionId
      .withColumn(partitionOffsetColName, mapPartitionIdToOffset(col(partitionIdColName)))
      .select(colsToSelect:_*)
      .drop(partitionIdColName, partitionOffsetColName, monotonicallyIncreasingIdColName)
  }

  /**
   * This implementation uses conversion to [[org.apache.spark.rdd.RDD]] and back to [[org.apache.spark.sql.DataFrame]]
   * @param offset the starting value of the identifier (included)
   * @param idColumnName the name of the identifier column
   * @param pos the position of a column relative to the existing columns
   * @param df a table to which the identifier will be added
   * @return data frame with continuous unique identifier column
   */
  def zipWithIndex(offset: Long = 1L,
                   idColumnName: String = "id",
                   pos: ColumnPosition = ColumnPosition.Last)
                  (df: DataFrame): DataFrame = {
    zipWith(_.zipWithIndex())(offset, idColumnName, pos)(df)
  }

  /**
   * This implementation uses conversion to [[org.apache.spark.rdd.RDD]] and back to [[org.apache.spark.sql.DataFrame]]
   * @param offset the starting value of the identifier (included)
   * @param idColumnName the name of the identifier column
   * @param pos the position of a column relative to the existing columns
   * @param df a table to which the identifier will be added
   * @return data frame with unique identifier column with possible discontinuities (gaps)
   */
  def zipWithUniqueId(offset: Long = 1L,
                      idColumnName: String = "id",
                      pos: ColumnPosition = ColumnPosition.Last)
                     (df: DataFrame): DataFrame = {
    zipWith(_.zipWithUniqueId())(offset, idColumnName, pos)(df)
  }

  private def zipWith(rddIdFunc: RDD[Row] => RDD[(Row, Long)])(offset: Long, idColumnName: String, pos: ColumnPosition)
                     (df: DataFrame): DataFrame = {
    val (schema, rdd) = pos match {
      case ColumnPosition.First => {
        (
          StructType(StructField(idColumnName, LongType) +: df.schema.fields),
          rddIdFunc(df.rdd).map(tp => Row.fromSeq((tp._2 + offset) +: tp._1.toSeq))
        )
      }
      case ColumnPosition.Last => {
        (
          StructType(df.schema.fields :+ StructField(idColumnName, LongType)),
          rddIdFunc(df.rdd).map(tp => Row.fromSeq(tp._1.toSeq :+ (tp._2 + offset)))
        )
      }
    }

    SparkSession.active.createDataFrame(rdd, schema)
  }
}
