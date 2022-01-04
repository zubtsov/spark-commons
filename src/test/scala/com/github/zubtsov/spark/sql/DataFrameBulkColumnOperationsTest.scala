package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison.assertEquals
import org.apache.spark.sql.Row
import com.github.zubtsov.spark.SparkSessionCommons.implicits._

class DataFrameBulkColumnOperationsTest extends SparkFunSuite {
  test("Test strings trimming without column names") {
    val source = spark.createDataFrame(
      "        `col1` STRING, `col2` INT",
      Row(       "   Orange",          1),
      Row(         "Beans  ",          2),
      Row(      "  Banana  ",          3),
      Row(         "Carrots",          4)
    )
    val expected = spark.createDataFrame(
      "        `col1` STRING, `col2` INT",
      Row(          "Orange",          1),
      Row(           "Beans",          2),
      Row(          "Banana",          3),
      Row(         "Carrots",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.trimStrings()

    assertEquals(expected, actual)
  }

  test("Test strings trimming with column names") {
    val source = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(       "   Orange",   "   Orange",          1),
      Row(         "Beans  ",     "Beans  ",          2),
      Row(      "  Banana  ",  "  Banana  ",          3),
      Row(         "Carrots",     "Carrots",          4)
    )
    val expected = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(          "Orange",   "   Orange",          1),
      Row(           "Beans",     "Beans  ",          2),
      Row(          "Banana",  "  Banana  ",          3),
      Row(         "Carrots",     "Carrots",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.trimStrings(Seq("col1"))

    assertEquals(expected, actual)
  }

  test("Test strings trimming with column names castSensitive = true") {
    val source = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(       "   Orange",   "   Orange",          1),
      Row(         "Beans  ",     "Beans  ",          2),
      Row(      "  Banana  ",  "  Banana  ",          3),
      Row(         "Carrots",     "Carrots",          4)
    )
    val expected = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(          "Orange",   "   Orange",          1),
      Row(           "Beans",     "Beans  ",          2),
      Row(          "Banana",  "  Banana  ",          3),
      Row(         "Carrots",     "Carrots",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.trimStrings(Seq("col1", "COL2"), caseSensitive = true)

    assertEquals(expected, actual)
  }

  test("Test strings trimming with column names castSensitive = false") {
    val source = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(       "   Orange",   "   Orange",          1),
      Row(         "Beans  ",     "Beans  ",          2),
      Row(      "  Banana  ",  "  Banana  ",          3),
      Row(         "Carrots",     "Carrots",          4)
    )
    val expected = spark.createDataFrame(
      "        `col1` STRING, `col2` STRING, `col3` INT",
      Row(          "Orange",      "Orange",          1),
      Row(           "Beans",       "Beans",          2),
      Row(          "Banana",      "Banana",          3),
      Row(         "Carrots",     "Carrots",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.trimStrings(Seq("col1", "COL2"), caseSensitive = false)

    assertEquals(expected, actual)
  }

  test("Test strings cutting") {
    val source = spark.createDataFrame(
      "`col1` STRING, `col2` STRING,  `col3` STRING, `col4` INT",
      Row(          "Orange",      "Orange",       "Orange",         1),
      Row(           "Beans",       "Beans",        "Beans",         2),
      Row(          "Banana",      "Banana",       "Banana",         3),
      Row(         "Carrots",     "Carrots",      "Carrots",         4)
    )
    val expected = spark.createDataFrame(
      "`col1` STRING, `col2` STRING, `col3` STRING, `col4` INT",
      Row(               "O",           "O",           "O",          1),
      Row(               "B",           "B",           "B",          2),
      Row(               "B",           "B",           "B",          3),
      Row(               "C",           "C",           "C",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.cutStrings(Seq("col1", "col2", "col3"), 1)

    assertEquals(expected, actual)
  }

  test("Test strings cutting 2") {
    val source = spark.createDataFrame(
      "`col1` STRING, `col2` STRING,  `col3` STRING, `col4` INT",
      Row(          "Orange",      "Orange",       "Orange",         1),
      Row(           "Beans",       "Beans",        "Beans",         2),
      Row(          "Banana",      "Banana",       "Banana",         3),
      Row(         "Carrots",     "Carrots",      "Carrots",         4)
    )
    val expected = spark.createDataFrame(
      "`col1` STRING, `col2` STRING, `col3` STRING, `col4` INT",
      Row(               "O",          "Or",         "Ora",          1),
      Row(               "B",          "Be",         "Bea",          2),
      Row(               "B",          "Ba",         "Ban",          3),
      Row(               "C",          "Ca",         "Car",          4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.cutStrings2(Map("col1" -> 1, "col2" -> 2, "col3" -> 3))

    assertEquals(expected, actual)
  }

  test("Test regexp replace") {
    val source = spark.createDataFrame(
      "`col1` STRING, `col2` STRING,  `col3` STRING, `col4` INT",
      Row(          "Orange",      "Orange",       "Orange",         1),
      Row(           "Beans",       "Beans",        "Beans",         2),
      Row(          "Banana",      "Banana",       "Banana",         3),
      Row(         "Carrots",     "Carrots",      "Carrots",         4)
    )
    val expected = spark.createDataFrame(
      "`col1` STRING, `col2` STRING,  `col3` STRING, `col4` INT",
      Row(          "Oxxxxx",      "Oxxxxx",       "Oxxxxx",         1),
      Row(           "Bxxxx",       "Bxxxx",        "Bxxxx",         2),
      Row(          "Bxxxxx",      "Bxxxxx",       "Bxxxxx",         3),
      Row(         "Cxxxxxx",     "Cxxxxxx",      "Cxxxxxx",         4)
    )

    import DataFrameBulkColumnOperations.implicits._
    val actual = source.regexpReplace("[a-z]", "x", Seq("col1", "col2", "col3"))

    assertEquals(expected, actual)
  }
}
