package com.github.zubtsov.spark

import SparkSessionCommons.implicits._

class DataFrameComparisonTest extends SparkFunSuite {
  test("Column names difference case insensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE")
    assert(DataFrameComparison.getColumnNamesDifference(df1, df2, caseSensitive = true).isTheSame())
  }

  test("Column names difference case sensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE")
    val difference = DataFrameComparison.getColumnNamesDifference(df1, df2, caseSensitive = true)
    assert(difference.isDifferent())
    assert(difference.leftMissingCols.size == 2)
    assert(difference.rightMissingCols.size == 2)
  }

}
