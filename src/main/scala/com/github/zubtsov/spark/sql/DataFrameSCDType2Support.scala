package com.github.zubtsov.spark.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//TODO: add joins?
//TODO: support soft delete expression
//TODO: add compact changes method? (consider gaps)
object DataFrameSCDType2Support {

  object implicits {

    implicit class DataFrameSCDType2SupportImpl(df: DataFrame) {
      /**
       * Converts periodic snapshot table to SCD type 2 + type 1 table and merges time periods/intervals where no SCD type 2 attributes changed.
       * Active records can be found using end_date = null condition.
       *
       * @param primaryKeyColNames
       * @param scdType2ColNames
       * @param businessDateColName essentially effective/start date of a row
       * @param startDateColName
       * @param endDateColName
       * @return
       */
      def periodicSnapshotToSCD2(primaryKeyColNames: Seq[String],
                                 scdType2ColNames: Seq[String], //all other columns considered as type 1
                                 businessDateColName: String,
                                 startDateColName: String = "start_date",
                                 endDateColName: String = "end_date"): DataFrame = {
        val groupedSCD2RowIndex = "tmp_grouped_scd2_row_index_"
        val previousRowSCD2AttrsAreDifferent = "_tmp_prev_row_scd2_attrs_diff_"
        val lastRowOfSCD2Group = "_tmp_last_row_of_scd2_group_"

        val w = Window.partitionBy(primaryKeyColNames.head, primaryKeyColNames.tail: _*)
          .orderBy(startDateColName)
        val w2 = Window.partitionBy(primaryKeyColNames.head, primaryKeyColNames.tail: _*)
          .orderBy(startDateColName)
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        val w3 = Window.partitionBy(groupedSCD2RowIndex, primaryKeyColNames: _*)

        df
          .withColumnRenamed(businessDateColName, startDateColName)
          .withColumn(endDateColName, lead(startDateColName, 1).over(w))
          .withColumn(previousRowSCD2AttrsAreDifferent, when(scdType2ColNames.map(cn => {
            col(cn) =!= lag(cn, 1).over(w) or lag(cn, 1).over(w).isNull
          }).reduce(_ or _), lit(1)).otherwise(lit(0))
          )
          .withColumn(groupedSCD2RowIndex, sum(previousRowSCD2AttrsAreDifferent).over(w2))
          .withColumn(lastRowOfSCD2Group, lead(groupedSCD2RowIndex, 1).over(w) =!= col(groupedSCD2RowIndex) or lead(groupedSCD2RowIndex, 1).over(w).isNull)
          .withColumn(startDateColName, min(startDateColName).over(w3))
          .where(col(lastRowOfSCD2Group))
          .drop(previousRowSCD2AttrsAreDifferent, groupedSCD2RowIndex, lastRowOfSCD2Group)
      }
    }
  }
}
