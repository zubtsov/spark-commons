package com.github.zubtsov.spark

import SparkSessionCommons.implicits._
import com.github.zubtsov.spark.DataFrameComparison.{JoinStatistics, assertEqualData, assertEquals}
import com.github.zubtsov.spark.exception.EmptyRowException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField}

class DataFrameComparisonTest extends SparkFunSuite {
  test("Column names difference case insensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = false)
    assert(difference.isDifferent);
    assert(difference.leftMissingCols == Seq("col5"))
    assert(difference.rightMissingCols == Seq("col4"))
    assert(!DataFrameComparison.hasEqualColumnNames(df1.columns, df2.columns, caseSensitive = false))
  }

  test("Column names difference case sensitive") {
    val df1 = spark.emptyDataFrame2("col1 STRING, col2 STRING, col3 DOUBLE, col4 INT")
    val df2 = spark.emptyDataFrame2("col1 INT, COL2 INT, COL3 DOUBLE, col5 INT")
    val difference = DataFrameComparison.getColumnNamesDifference(df1.columns, df2.columns, caseSensitive = true)
    assert(difference.isDifferent)
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

  test("Data difference approximate") {
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.1,          BigDecimal(1.1)),
      Row(         "str2",                1.2,          BigDecimal(1.2)),
      Row(         "str3",                1.3,          BigDecimal(1.3)),
      Row(         "str4",                1.4,          BigDecimal(1.4)),
      Row(         "str5",                1.5,          BigDecimal(1.5)),
      Row(         "str6",                1.6,          BigDecimal(1.6))
    )
    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.06,         BigDecimal(1.1)),
      Row(         "str2",                1.24,         BigDecimal(1.2)),
      Row(         "str3",                1.4,          BigDecimal(1.3)),
      Row(         "str4",                1.4,          BigDecimal(1.36)),
      Row(         "str5",                1.5,          BigDecimal(1.54)),
      Row(         "str6",                1.6,          BigDecimal(1.7))
    )
    val difference = DataFrameComparison.getDataDifferenceApproximate(df1, df2, Seq("double_val", "decimal_val"), 0.05)
    assert(difference.leftMissingRows.collect().toSet == Set(
      Row(         "str3",                1.4,          new java.math.BigDecimal("1.3000")),
      Row(         "str6",                1.6,          new java.math.BigDecimal("1.7000"))
    ))
    assert(difference.rightMissingRows.collect().toSet == Set(
      Row(         "str3",                1.3,          new java.math.BigDecimal("1.3000")),
      Row(         "str6",                1.6,          new java.math.BigDecimal("1.6000"))
    ))
  }

  test("Data difference approximate 2") {
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.1,          BigDecimal(1.1)),
      Row(         "str2",                1.2,          BigDecimal(1.2)),
      Row(         "str3",                1.3,          BigDecimal(1.3)),
      Row(         "str4",                1.4,          BigDecimal(1.4)),
      Row(         "str5",                1.5,          BigDecimal(1.5)),
      Row(         "str6",                1.6,          BigDecimal(1.6))
    )
    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.2,         BigDecimal(1.101)),
      Row(         "str2",                1.4,         BigDecimal(1.202)),
      Row(         "str3",                1.6,         BigDecimal(1.303)),
      Row(         "str4",                1.8,         BigDecimal(1.404)),
      Row(         "str5",                2.0,         BigDecimal(1.505)),
      Row(         "str6",                2.2,         BigDecimal(1.606))
    )
    val difference = DataFrameComparison.getDataDifferenceApproximate(df1, df2, Seq("string_val"), Map("double_val" -> 0.6, "decimal_val" -> 0.05))
    assert(difference.leftMissingRows.collect().toSet == Set(
      Row(         "str6",                2.2,          new java.math.BigDecimal("1.6060"))
    ))
    assert(difference.rightMissingRows.collect().toSet == Set(
      Row(         "str6",                1.6,          new java.math.BigDecimal("1.6000"))
    ))

    val difference2 = DataFrameComparison.getDataDifferenceApproximate(df1, df2, Seq("string_val"), Map("double_val" -> 0.5, "decimal_val" -> 0.06))
    assert(difference2.leftMissingRows.collect().toSet == Set(
      Row(         "str6",                2.2,          new java.math.BigDecimal("1.6060"))
    ))
    assert(difference2.rightMissingRows.collect().toSet == Set(
      Row(         "str6",                1.6,          new java.math.BigDecimal("1.6000"))
    ))
  }

  test("Data difference with duplicates") {
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.1,          BigDecimal(1.1)),
      Row(         "str2",                1.2,          BigDecimal(1.2)),
      Row(         "str3",                1.3,          BigDecimal(1.3)),
      Row(         "str3",                1.3,          BigDecimal(1.3)),
      Row(         "str3",                1.3,          BigDecimal(1.3))
    )

    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE, decimal_val DECIMAL(18,4)",
      Row(         "str1",                1.1,          BigDecimal(1.1)),
      Row(         "str2",                1.2,          BigDecimal(1.2)),
      Row(         "str3",                1.3,          BigDecimal(1.3)),
      Row(         "str3",                1.3,          BigDecimal(1.3))
    )

    val difference = DataFrameComparison.getDataDifference(df1, df2)
    assert(difference.leftMissingRows.count() == 0)
    assert(difference.rightMissingRows.collect.toSet == Set(
      Row(         "str3",                1.3,          new java.math.BigDecimal("1.3000"))
    ))
  }

  test("Collect join statistics") {
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1),
      Row(         "str2",              1.2),
      Row(         "str3",              1.3),
      Row(         "str4",              1.5),
      Row(         "str6",              1.6),
      Row(         "str6",              1.7),
      Row(         "str7",              1.9),
      Row(        "str10",              1.0),
      Row(        "str10",              1.1)
    )

    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",             1.11),
      Row(         "str1",             1.12),
      Row(         "str2",             1.21),
      Row(         "str3",             1.71),
      Row(         "str3",             1.31),
      Row(         "str5",             1.91),
      Row(         "str6",             1.41),
      Row(         "str8",             1.61),
      Row(         "str9",             1.91),
      Row(        "str10",             1.01)
    )

    val joinedLeft = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1),
      Row(         "str2",              1.2),
      Row(         "str3",              1.3),
      Row(         "str6",              1.6),
      Row(         "str6",              1.7),
      Row(        "str10",              1.0),
      Row(        "str10",              1.1)
    )
    val duplicatedLeft = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1),
      Row(         "str3",              1.3),
    )

    val joinedRight = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",             1.11),
      Row(         "str1",             1.12),
      Row(         "str2",             1.21),
      Row(         "str3",             1.71),
      Row(         "str3",             1.31),
      Row(         "str6",             1.41),
      Row(        "str10",             1.01)
    )
    val duplicatedRight = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str6",             1.41),
      Row(        "str10",             1.01),
    )

    val expected = new JoinStatistics(7, 2, 2, 7, 3, 2, joinedLeft, duplicatedLeft, joinedRight, duplicatedRight)
    val joinStatistics = DataFrameComparison.getJoinStatistics(df1, df2, df1("string_val") === df2("string_val"))
    assert(expected.toString == joinStatistics.toString)
    assertEquals(joinedLeft, joinStatistics.leftJoinedRows)
    assertEquals(joinedRight, joinStatistics.rightJoinedRows)
    assertEquals(duplicatedLeft, joinStatistics.leftDuplicatedRows)
    assertEquals(duplicatedRight, joinStatistics.rightDuplicatedRows)
  }

  test("Should be thrown EmptyRowException with right df as reference"){
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1)
    )

    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",             1.11),
      Row(           null,             null)
    )
    val exception = intercept[EmptyRowException] {
      DataFrameComparison.getJoinStatistics(df1, df2, df1("string_val") === df2("string_val"))
    }
    assert(exception.getMessage.equals("The RIGHT dataset has empty rows."))
  }

  test("Should be thrown EmptyRowException with left df as reference"){
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1)
    )

    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",             1.11),
      Row(           null,             null)
    )
    val exception = intercept[EmptyRowException] {
      DataFrameComparison.getJoinStatistics(df2, df1, df2("string_val") === df1("string_val"))
    }

    assert(exception.getMessage.equals("The LEFT dataset has empty rows."))
  }

  test("Should be thrown EmptyRowException with both df's as reference"){
    val df1 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",              1.1),
      Row(           null,             null)
    )

    val df2 = spark.createDataFrame(
      "string_val STRING, double_val DOUBLE",
      Row(         "str1",             1.11),
      Row(           null,             null)
    )
    val exception = intercept[EmptyRowException] {
      DataFrameComparison.getJoinStatistics(df1, df2, df2("string_val") === df1("string_val"))
    }
    assert(exception.getMessage.equals("BOTH datasets have empty rows."))
  }
}
