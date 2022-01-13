package com.github.zubtsov.spark.sql.column

import com.github.zubtsov.spark.SparkFunSuite
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import org.apache.spark.sql.Row

class ColumnCommonsTest extends SparkFunSuite {
  test("Test substring") {
    val source = spark.createDataFrame(
      "`str_val` STRING, `str_pos` INT, `str_length` INT",
      Row( "0123456789",             0,               10),
      Row( "0123456789",             1,                8),
      Row( "0123456789",             2,                6),
      Row( "0123456789",             3,                4),
      Row( "0123456789",             4,                2),
      Row( "0123456789",             5,                1)
    )
    import ColumnCommons._
    import org.apache.spark.sql.functions.col
    val actual = source.withColumn("str_result", substring(col("str_val"), col("str_pos"), col("str_length")))
    val expected = spark.createDataFrame(
      "   `str_val` STRING, `str_pos` INT, `str_length` INT, `str_result` STRING",
      Row(    "0123456789",             0,               10,        "0123456789"),
      Row(    "0123456789",             1,                8,          "01234567"),
      Row(    "0123456789",             2,                6,            "123456"),
      Row(    "0123456789",             3,                4,              "2345"),
      Row(    "0123456789",             4,                2,                "34"),
      Row(    "0123456789",             5,                1,                 "4")
    )
    import com.github.zubtsov.spark.DataFrameComparison._
    assertEquals(actual, expected)
  }
}
