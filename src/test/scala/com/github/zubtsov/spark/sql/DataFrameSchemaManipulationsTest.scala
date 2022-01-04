package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison._
import com.github.zubtsov.spark.SparkFunSuite
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import com.github.zubtsov.spark.exception.UnknownColumnsException
import com.github.zubtsov.spark.sql.DataFrameSchemaManipulations.implicits._
import org.apache.spark.sql.Row
//TODO: add more test cases
//TODO: test not empty data frames
class DataFrameSchemaManipulationsTest extends SparkFunSuite {
  test("Trim column names case insensitive") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    val expected = spark.emptyDataFrame2("`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING")
    val actual = source.withTrimmedColumnNames(Seq("col_name1", "COL_NAME2", "col_name3"), false)
    assertEqualSchemas(expected, actual, true, true)
  }

  test("Trim column names case sensitive") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    val expected = spark.emptyDataFrame2("`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING")
    val actual = source.withTrimmedColumnNames(Seq("col_name1", "col_name2", "col_name3"), true)
    assertEqualSchemas(expected, actual, true, true)
  }

  test("Trim column names with non-existing column case insensitive") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    assertThrows[UnknownColumnsException](source.withTrimmedColumnNames(Seq("col_name1", "COL_NAME3", "random_col"), false))
  }

  test("Trim column names with non-existing column") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    assertThrows[UnknownColumnsException](source.withTrimmedColumnNames(Seq("col_name1 ", "COL_NAME3"), true))
  }

  test("Unicode trim column names test") {
    val source = spark.emptyDataFrame2("`\u00a0col_name1 ` STRING, ` col_name2\u00a0` INT, ` \t\u00a0col_name3\u00a0\t  ` DOUBLE, ` do not trim ` STRING")
    val expected = spark.emptyDataFrame2("`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING")
    val actual = source.withUnicodeTrimmedColumnNames(Seq("col_name1", "COL_NAME2", "col_name3"), false)
    assertEqualSchemas(expected, actual, true, true)
  }

  test("Columns are sorted") {
    val source = spark.emptyDataFrame2("d STRING, b STRING, a STRING, c STRING")
    val expected = spark.emptyDataFrame2("a STRING, b STRING, c STRING, d STRING")
    val actual = source.sortColumns(identity)
    assertEqualSchemas(expected, actual)
  }

  test("Schema is casted") {
    val source = spark.createDataFrame(
      "a STRING, b STRING, c STRING",
      Row(  "1",    "1.0",    "true"),
      Row(  "2",    "2.0",   "false"),
      Row(  "3",    "3.0",        "")
    )

    val expected = spark.createDataFrame(
      "a INT, b DOUBLE, c BOOLEAN",
      Row( 1,      1.0,      true),
      Row( 2,      2.0,     false),
      Row( 3,      3.0,      null)
    )

    val actual = source.cast(expected.schema)

    assertEquals(expected, actual)
  }
}
