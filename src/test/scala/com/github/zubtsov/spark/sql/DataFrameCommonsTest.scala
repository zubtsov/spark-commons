package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison._
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import org.apache.spark.sql.Row

class DataFrameCommonsTest extends SparkFunSuite {
  test("Unpivot test") {
    val source = spark.createDataFrame(
      "   `Product` STRING, `Country1` STRING, `Country2` STRING, `Country3` STRING, `Country4` STRING, `SomeInt` INT",
      Row(        "Orange",              "O1",              "O2",              "O3",              "O4",             1),
      Row(         "Beans",              "B1",              "B2",              "B3",              "B4",             2),
      Row(        "Banana",              "B1",              "B2",              "B3",              "B4",             3),
      Row(       "Carrots",              "C1",              "C2",              "C3",              "C4",             4)
    )
    val expected = spark.createDataFrame(
      "   `Product` STRING, `SomeInt` INT, `CountryName` STRING, `Value` STRING",
      Row(        "Orange",             1,           "Country1",           "O1"),
      Row(        "Orange",             1,           "Country2",           "O2"),
      Row(        "Orange",             1,           "Country3",           "O3"),
      Row(        "Orange",             1,           "Country4",           "O4"),
      Row(         "Beans",             2,           "Country1",           "B1"),
      Row(         "Beans",             2,           "Country2",           "B2"),
      Row(         "Beans",             2,           "Country3",           "B3"),
      Row(         "Beans",             2,           "Country4",           "B4"),
      Row(        "Banana",             3,           "Country1",           "B1"),
      Row(        "Banana",             3,           "Country2",           "B2"),
      Row(        "Banana",             3,           "Country3",           "B3"),
      Row(        "Banana",             3,           "Country4",           "B4"),
      Row(       "Carrots",             4,           "Country1",           "C1"),
      Row(       "Carrots",             4,           "Country2",           "C2"),
      Row(       "Carrots",             4,           "Country3",           "C3"),
      Row(       "Carrots",             4,           "Country4",           "C4")
    )
    import DataFrameCommons.implicits._
    val actual = source.unpivot(Seq("Country1", "Country2", "Country3", "Country4"), "CountryName", "Value")
    assertEquals(expected, actual)
  }
}
