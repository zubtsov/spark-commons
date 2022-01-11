package com.github.zubtsov.spark

import SparkSessionCommons.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

//TODO: add tests (cover positive cases)
class DataFrameComparisonTest extends SparkFunSuite {
  test("Column names difference case insensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = false)
    assert(difference.isDifferent())
    assert(difference.leftMissingCols == Seq("col5"))
    assert(difference.rightMissingCols == Seq("col4"))
    assert(!DataFrameComparison.hasEqualColumnNames(df1.columns, df2.columns, caseSensitive = false))
  }

  test("Column names difference case sensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = true)
    assert(difference.isDifferent())
    assert(difference.leftMissingCols == Seq("COL2", "COL3", "col5"))
    assert(difference.rightMissingCols == Seq("col2", "col3", "col4"))
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

  test("Data difference") {
    val df1 = spark.createDataFrame(
      "string_val1 STRING, int_val1 INT, double_val1 DOUBLE, boolean_val BOOLEAN",
      Row(         "str2",            2,                2.0,               false),
      Row(         "str3",            3,                3.0,                true)
    )
    val df2 = spark.createDataFrame(
      "string_val1 STRING, int_val1 INT, double_val1 DOUBLE, boolean_val BOOLEAN",
      Row(         "str1",            1,                1.0,                true),
      Row(         "str2",            2,                2.0,               false)
    )
    val difference = DataFrameComparison.getDataDifference(df1, df2)
    assert(DataFrameComparison.hasDifferentData(df1, df2))
    assert(difference.leftMissingRows.collect().toSeq == Seq(Row("str1", 1, 1.0, true)))
    assert(difference.rightMissingRows.collect().toSeq == Seq(Row("str3", 3, 3.0, true)))
  }
}
