package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.enums.JoinType
import com.github.zubtsov.spark.enums.JoinType.JoinType
import com.github.zubtsov.spark.zubtsov.defaultCaseSensitivity
import org.apache.spark.sql.{Column, DataFrame}
//TODO: implement
object DataFrameSCDType2Support {
  implicit class DataFrameSCDType2Support(df: DataFrame) {
    private def joinWithSCD2(other: DataFrame, usingColumns: Seq[String], joinType: JoinType = JoinType.Inner): DataFrame = {
      ???
    }

    private def joinWithSCD2(other: DataFrame, joinExpr: Column, joinType: JoinType): DataFrame = {
      ???
    }

    private def convert(primaryKeyColNames: Seq[String],
                scdType2ColNames: Seq[String], //all other columns considered as type 1
                startDateColName: String = "start_date",
                endDateColName: String = "end_date",
                pkColNamesCaseSensitive: Boolean = defaultCaseSensitivity,
                stringAttrsCaseSensitive: Boolean = defaultCaseSensitivity,
                trimStringType2Cols: Boolean = false): DataFrame = {
      ???
    }
  }
}
