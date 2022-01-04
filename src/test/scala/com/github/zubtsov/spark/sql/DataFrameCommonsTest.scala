package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.DataFrameComparison._
import com.github.zubtsov.spark.SparkFunSuite
import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import com.github.zubtsov.spark.enums.{ColumnPosition, UnionStrategy}
import com.github.zubtsov.spark.exception.ColumnAlreadyExistsException
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

  test("Unpivot test 2") {
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
    val actual = source.unpivot2(Seq("Country1", "Country2", "Country3", "Country4"), "CountryName", "Value")
    assertEquals(expected, actual)
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

  test("Union test 1") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `col2` STRING, `col3` STRING",
      Row(           "c",           "d")
    )

    val expected = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING, `col3` STRING",
      Row(           "a",           "b",          null),
      Row(          null,           "c",           "d"),
    )
    import DataFrameCommons.implicits._
    val actual = t1.union2(t2)

    assertEquals(actual, expected)
  }

  test("Union test 2") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `col2` STRING, `col3` STRING",
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

  test("Union test 3") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `col2` STRING, `col3` STRING",
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

  test("Union test 4") {
    val t1 = spark.createDataFrame(
      "    `col1` STRING, `col2` STRING",
      Row(           "a",           "b")
    )
    val t2 = spark.createDataFrame(
      "    `col2` STRING, `col3` STRING",
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

  test("With column test") {
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
    val actual = source.withColumn2("col2", lit("b"))

    assertEquals(actual, expected)
  }

  test("With column test 2") {
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
    val actual = source.withColumn2("col2", lit("b"), pos = ColumnPosition.Head)

    assertEquals(actual, expected)
  }

  test("With column test 3") {
    val source = spark.createDataFrame(
      "    `col1` STRING",
      Row(           "a")
    )

    import org.apache.spark.sql.functions.lit
    import DataFrameCommons.implicits._

    assertThrows[ColumnAlreadyExistsException](source.withColumn2("col1", lit(null)))
  }

  test("Drop duplicates ignore case") {
    val source = spark.createDataFrame(
      "    `col1` STRING",
      Row(           "a"),
      Row(           "A"),
      Row(           "b"),
      Row(           "b"),
      Row(           "c")
    )
    //todo: check data using contains all/any methods for comparison
    import DataFrameCommons.implicits._

    assertResult(3)(source.dropDuplicatesIgnoreCase().count())
  }
}
