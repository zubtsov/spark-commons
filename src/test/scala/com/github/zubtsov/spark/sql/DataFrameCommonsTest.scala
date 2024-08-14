package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison._
import com.github.zubtsov.spark.{DataFrameComparison, SparkFunSuite}
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import com.github.zubtsov.spark.enums.{ColumnPosition, UnionStrategy}
import com.github.zubtsov.spark.exception.ColumnAlreadyExistsException
import org.apache.spark.sql.Row

class DataFrameCommonsTest extends SparkFunSuite {
  test("Unpivot test caseSensitive = false") {
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
    val actual = source.unpivot2(Seq("Country1", "Country2", "Country3", "COUNTRY4"), "CountryName", "Value", caseSensitive = false)
    assertEquals(expected, actual)
    val actual2 = source.unpivot3(Seq("Country1", "Country2", "Country3", "COUNTRY4"), "CountryName", "Value", caseSensitive = false)
    assertEquals(expected, actual2)
  }

  test("Unpivot test caseSensitive = true") {
    val source = spark.createDataFrame(
      "   `Product` STRING, `Country1` STRING, `Country2` STRING, `Country3` STRING, `Country4` STRING, `SomeInt` INT",
      Row(        "Orange",              "O1",              "O2",              "O3",              "O4",             1),
      Row(         "Beans",              "B1",              "B2",              "B3",              "B4",             2),
      Row(        "Banana",              "B1",              "B2",              "B3",              "B4",             3),
      Row(       "Carrots",              "C1",              "C2",              "C3",              "C4",             4)
    )
    val expected = spark.createDataFrame(
      "   `Product` STRING, `SomeInt` INT, `CountryName` STRING, `Value` STRING, `Country4` STRING",
      Row(        "Orange",             1,           "Country1",           "O1",               "O4"),
      Row(        "Orange",             1,           "Country2",           "O2",               "O4"),
      Row(        "Orange",             1,           "Country3",           "O3",               "O4"),
      Row(         "Beans",             2,           "Country1",           "B1",               "B4"),
      Row(         "Beans",             2,           "Country2",           "B2",               "B4"),
      Row(         "Beans",             2,           "Country3",           "B3",               "B4"),
      Row(        "Banana",             3,           "Country1",           "B1",               "B4"),
      Row(        "Banana",             3,           "Country2",           "B2",               "B4"),
      Row(        "Banana",             3,           "Country3",           "B3",               "B4"),
      Row(       "Carrots",             4,           "Country1",           "C1",               "C4"),
      Row(       "Carrots",             4,           "Country2",           "C2",               "C4"),
      Row(       "Carrots",             4,           "Country3",           "C3",               "C4"),
    )
    import DataFrameCommons.implicits._
    val actual = source.unpivot2(Seq("Country1", "Country2", "Country3", "COUNTRY4"), "CountryName", "Value", caseSensitive = true)
    assertEquals(expected, actual)
    val actual2 = source.unpivot3(Seq("Country1", "Country2", "Country3", "COUNTRY4"), "CountryName", "Value", caseSensitive = true)
    assertEquals(expected, actual2)
  }

  test("Sampling test") {
    import DataFrameCommons.implicits._

    for (tableSize <- 100 to 110) {
      for (sampleSize <- 1 to 10) {
        println(s"Sampling ${sampleSize} records out of ${tableSize}")
        val source = spark.range(tableSize).toDF("id")
        val sampled = source.sample2(sampleSize)
        assertResult(sampleSize)(sampled.count())
      }
    }
  }

  test("Union test all columns") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `COL2` STRING, `col3` STRING",
      Row(           "c",           "d")
    )

    val expected = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING, `col3` STRING",
      Row(           "a",           "b",          null),
      Row(          null,           "c",           "d"),
    )
    import DataFrameCommons.implicits._
    val actual = t1.union2(t2, unionStrategy = UnionStrategy.AllColumns)

    assertEquals(actual, expected)
  }

  test("Union test left columns") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `COL2` STRING, `col3` STRING",
      Row(           "c",           "d")
    )

    val expected = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b"),
      Row(          null,           "c"),
    )
    import DataFrameCommons.implicits._
    val actual = t1.union2(t2, UnionStrategy.LeftColumns)

    assertEquals(actual, expected)
  }

  test("Union test right columns") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `COL2` STRING, `col3` STRING",
      Row(           "c",           "d")
    )

    val expected = spark.createDataFrame(
      "    `col2` STRING, `col3` STRING",
      Row(           "b",          null),
      Row(           "c",           "d"),
    )
    import DataFrameCommons.implicits._
    val actual = t1.union2(t2, UnionStrategy.RightColumns)

    assertEquals(actual, expected)
  }

  test("Union test common columns") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `COL2` STRING, `col3` STRING",
      Row(           "c",           "d")
    )

    val expected = spark.createDataFrame(
      "   `col2` STRING",
      Row(          "b"),
      Row(          "c"),
    )
    import DataFrameCommons.implicits._
    val actual = t1.union2(t2, UnionStrategy.CommonColumns)

    assertEquals(actual, expected)
  }

  test("withColumn2 adds last column") {
    val source = spark.createDataFrame(
      "    `col1` STRING",
      Row(           "a")
    )

    val expected = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",            "b")
    )
    import org.apache.spark.sql.functions.lit
    import DataFrameCommons.implicits._
    val actual = source.withColumn2("col2", lit("b"), pos = ColumnPosition.Last)

    assertEquals(actual, expected)
  }

  test("withColumn2 adds first column") {
    val source = spark.createDataFrame(
      "    `col1` STRING",
      Row(           "a")
    )

    val expected = spark.createDataFrame(
      "    `col2` STRING, `col1` STRING",
      Row(           "b",            "a")
    )
    import org.apache.spark.sql.functions.lit
    import DataFrameCommons.implicits._
    val actual = source.withColumn2("col2", lit("b"), pos = ColumnPosition.First)

    assertEquals(actual, expected)
  }

  test("withColumn2 throws exception in case column already exists") {
    val source = spark.createDataFrame(
      "    `col1` STRING",
      Row(           "a")
    )

    import org.apache.spark.sql.functions.lit
    import DataFrameCommons.implicits._

    assertThrows[ColumnAlreadyExistsException](source.withColumn2("COL1", lit(null)))
  }

  test("Drop duplicates ignore case caseSensitive = false") {
    val source = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING, `col3` INT",
      Row(           "a",           "a",          1),
      Row(           "A",           "b",          2),
      Row(           "b",           "c",          3),
      Row(           "b",           "C",          3),
      Row(           "c",           "d",          5),
      Row(           "c",           "d",          6)
    )

    import DataFrameCommons.implicits._
    val actual = source.dropDuplicatesIgnoreCase(Seq("COL1", "COL2", "col3"), caseSensitive = false)
    assertResult(5)(actual.count())

    actual.cache() //FIXME: workaround because of Spark bug, find or report it

    val difference = DataFrameComparison.getDataDifference(source, actual)
    assert(difference.leftMissingRows.isEmpty)
    assert(difference.rightMissingRows.count() == 1)
  }

  test("Drop duplicates ignore case caseSensitive = true") {
    val source = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "a"),
      Row(           "A",           "b"),
      Row(           "b",           "c"),
      Row(           "b",           "C"),
      Row(           "c",           "d")
    )

    import DataFrameCommons.implicits._
    val actual = source.dropDuplicatesIgnoreCase(Seq("col1", "COL2"), caseSensitive = true)
    assertResult(3)(actual.count())

    actual.cache() //FIXME: workaround because of Spark bug, find or report it

    val difference = DataFrameComparison.getDataDifference(source, actual)
    assert(difference.leftMissingRows.isEmpty)
    assert(difference.rightMissingRows.count() == 2)
  }

}
