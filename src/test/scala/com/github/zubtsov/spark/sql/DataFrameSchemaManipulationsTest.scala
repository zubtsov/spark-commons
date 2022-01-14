package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison._
import com.github.zubtsov.spark.SparkFunSuite
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import com.github.zubtsov.spark.exception.UnknownColumnsException
import com.github.zubtsov.spark.sql.DataFrameSchemaManipulations.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
//TODO: add more test cases

class DataFrameSchemaManipulationsTest extends SparkFunSuite {
  test("Trim column names case insensitive") {
    val source = spark.createDataFrame(
      "`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val expected = spark.createDataFrame(
      "`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val actual = source.withTrimmedColumnNames(Seq("col_name1", "COL_NAME2", "col_name3"), false)
    assertEquals(expected, actual, true, true)
  }

  test("Trim column names case sensitive") {
    val source = spark.createDataFrame(
      "`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val expected = spark.createDataFrame(
      "`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val actual = source.withTrimmedColumnNames(Seq("col_name1", "col_name2", "col_name3"), true)
    assertEquals(expected, actual, true, true)
  }

  test("Trim column names with non-existing column case insensitive") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    assertThrows[UnknownColumnsException](source.withTrimmedColumnNames(Seq("col_name1", "COL_NAME3", "random_col"), false))
  }

  test("Trim column names with non-existing column") {
    val source = spark.emptyDataFrame2("`col_name1 ` STRING, ` col_name2` INT, ` \tcol_name3\t  ` DOUBLE, ` do not trim ` STRING")
    assertThrows[UnknownColumnsException](source.withTrimmedColumnNames(Seq("col_name1 ", "COL_NAME3"), true))
  }

  test("Unicode trim column names test case insensitive") {
    val source = spark.createDataFrame(
      "`\u00a0col_name1 ` STRING, ` col_name2\u00a0` INT, ` \t\u00a0col_name3\u00a0\t  ` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val expected = spark.createDataFrame(
      "`col_name1` STRING, `col_name2` INT, `col_name3` DOUBLE, ` do not trim ` STRING",
      Row("v1", 1, 1.0, "v2")
    )
    val actual = source.withUnicodeTrimmedColumnNames(Seq("col_name1", "COL_NAME2", "col_name3"), false)
    assertEquals(expected, actual, true, true)
  }

  test("Unicode trim column names test case sensitive") {
    val source = spark.emptyDataFrame2("`\u00a0col_name1 ` STRING, ` col_name2\u00a0` INT, ` \t\u00a0col_name3\u00a0\t  ` DOUBLE, ` do not trim ` STRING")
    assertThrows[UnknownColumnsException](source.withUnicodeTrimmedColumnNames(Seq("col_name1", "COL_NAME2", "col_name3"), true))
  }

  test("Columns are sorted") {
    val source = spark.createDataFrame("d STRING, b STRING, a STRING, c STRING",
      Row("d", "b", "a", "c")
    )
    val expected = spark.createDataFrame("a STRING, b STRING, c STRING, d STRING",
      Row("a", "b", "c", "d")
    )
    val actual = source.sortColumns(identity)
    assertEqualSchemas(expected, actual)
  }

  test("Schema is casted normal case") {
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

  test("Schema is casted missing columns added, excessive columns removed") {
    val source = spark.createDataFrame(
      "a STRING, b STRING, c STRING",
      Row(  "1",    "1.0",    "true"),
      Row(  "2",    "2.0",   "false"),
      Row(  "3",    "3.0",        "")
    )

    val expected = spark.createDataFrame(
      "a INT, b DOUBLE, d STRING",
      Row( 1,      1.0,     null),
      Row( 2,      2.0,     null),
      Row( 3,      3.0,     null)
    )

    val actual = source.cast(expected.schema, removeExcessiveColumns = true, addMissingColumns = true)
    assertEquals(expected, actual)
  }

  test("Cast schema removes excessive columns, but doesn't add missing columns") {
    val source = spark.createDataFrame(
      "a STRING, b STRING, c STRING",
      Row(  "1",    "1.0",    "true"),
      Row(  "2",    "2.0",   "false"),
      Row(  "3",    "3.0",        "")
    )

    val targetSchema = StructType.fromDDL("a INT, b DOUBLE, d STRING")
    val expected = spark.createDataFrame(
      "a INT, b DOUBLE",
      Row( 1,      1.0),
      Row( 2,      2.0),
      Row( 3,      3.0)
    )

    val actual = source.cast(targetSchema, removeExcessiveColumns = true, addMissingColumns = false)
    assertEquals(actual, expected)
  }

  test("Cast schema doesn't excessive columns, but adds missing columns") {
    val source = spark.createDataFrame(
      "a STRING, b STRING, c STRING",
      Row(  "1",    "1.0",    "true"),
      Row(  "2",    "2.0",   "false"),
      Row(  "3",    "3.0",        "")
    )

    val targetSchema = StructType.fromDDL("a INT, b DOUBLE, d STRING")
    val expected = spark.createDataFrame(
      "a INT, b DOUBLE, c STRING, d STRING",
      Row( 1,      1.0,   "true",     null),
      Row( 2,      2.0,  "false",     null),
      Row( 3,      3.0,       "",     null)
    )

    val actual = source.cast(targetSchema, removeExcessiveColumns = false, addMissingColumns = true)
    assertEquals(actual, expected)
  }

  test("Cast schema adds excessive columns, but doesn't add missing columns") {
    val source = spark.createDataFrame(
      "a STRING, b STRING, c STRING",
      Row(  "1",    "1.0",    "true"),
      Row(  "2",    "2.0",   "false"),
      Row(  "3",    "3.0",        "")
    )

    val targetSchema = StructType.fromDDL("a INT, b DOUBLE, d STRING")
    val expected = spark.createDataFrame(
      "a INT, b DOUBLE, c STRING",
      Row( 1,      1.0,   "true"),
      Row( 2,      2.0,  "false"),
      Row( 3,      3.0,       "")
    )

    val actual = source.cast(targetSchema, removeExcessiveColumns = false, addMissingColumns = false)
    assertEquals(actual, expected)
  }
}
