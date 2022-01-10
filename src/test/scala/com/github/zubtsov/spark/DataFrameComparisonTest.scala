package com.github.zubtsov.spark

import SparkSessionCommons.implicits._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

class DataFrameComparisonTest extends SparkFunSuite {
  test("Column names difference case insensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = false)
    assert(difference.leftMissingCols == Seq("col5"))
    assert(difference.rightMissingCols == Seq("col4"))
  }

  test("Column names difference case sensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = true)
    assert(difference.isDifferent())
    assert(difference.leftMissingCols.size == 3)
    assert(difference.rightMissingCols.size == 3)
  }

  test("Schema difference case insensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getSchemaDifference(df1.schema, df2.schema, caseSensitive = false)
    assert(difference.rightMissingFields == Seq(StructField("col1", StringType), StructField("col2", StringType), StructField("col4", IntegerType)))
    assert(difference.leftMissingFields == Seq(StructField("col1", IntegerType), StructField("COL2", IntegerType), StructField("col5", IntegerType)))
  }

  test("Schema difference case sensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 STRING, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getSchemaDifference(df1.schema, df2.schema, caseSensitive = true)
    assert(difference.rightMissingFields == Seq(StructField("col1", StringType), StructField("col2", StringType), StructField("col3", DoubleType), StructField("col4", IntegerType)))
    assert(difference.leftMissingFields == Seq(StructField("col1", IntegerType), StructField("COL2", StringType), StructField("COL3", DoubleType), StructField("col5", IntegerType)))
  }
}
