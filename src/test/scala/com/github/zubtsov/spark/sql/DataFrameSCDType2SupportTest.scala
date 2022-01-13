package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.SparkFunSuite
import com.github.zubtsov.spark.Literals.implicits._
import org.apache.spark.sql.Row
import com.github.zubtsov.spark.DataFrameComparison._

class DataFrameSCDType2SupportTest extends SparkFunSuite {
  test("Test SCD type 2 conversion") {
    import com.github.zubtsov.spark.SparkSessionCommons.implicits._

    val source = spark.createDataFrame(
      "`my_primary_key` STRING, `int_value` INT, `string_value` STRING, type1_attr STRING, `business_date` DATE",
      Row(               "pk1",               1,                  "v1",               "1",        d"2022-01-03"),
      Row(               "pk1",               1,                  "v1",               "1",        d"2022-01-04"),
      Row(               "pk1",               1,                  "v1",               "1",        d"2022-01-05"),
      Row(               "pk1",               1,                  "v1",               "1",        d"2022-01-06"),
      Row(               "pk1",               1,                  "v1",               "2",        d"2022-01-07"),
      Row(               "pk1",               2,                  "v1",               "3",        d"2022-01-08"),
      Row(               "pk1",               2,                  "v1",               "4",        d"2022-01-09"),
      Row(               "pk1",               2,                  "v2",               "5",        d"2022-01-10"),
      Row(               "pk1",               2,                  "v2",               "6",        d"2022-01-11"),
      Row(               "pk2",               3,                  "v3",               "7",        d"2022-01-06"),
      Row(               "pk2",               3,                  "v3",               "8",        d"2022-01-07"),
      Row(               "pk2",               4,                  "v3",               "9",        d"2022-01-08"),
      Row(               "pk2",               4,                  "v3",              "10",        d"2022-01-09"),
      Row(               "pk2",               4,                  "v4",              "11",        d"2022-01-10"),
      Row(               "pk2",               4,                  "v4",              "12",        d"2022-01-11")
    )

    val expected = spark.createDataFrame(
      "`my_primary_key` STRING, `int_value` INT, `string_value` STRING, type1_attr STRING, `start_date` DATE, `end_date` DATE",
      Row(               "pk1",               1,                  "v1",               "2",     d"2022-01-03",   d"2022-01-08"),
      Row(               "pk1",               2,                  "v1",               "4",     d"2022-01-08",   d"2022-01-10"),
      Row(               "pk1",               2,                  "v2",               "6",     d"2022-01-10",            null),
      Row(               "pk2",               3,                  "v3",               "8",     d"2022-01-06",   d"2022-01-08"),
      Row(               "pk2",               4,                  "v3",              "10",     d"2022-01-08",   d"2022-01-10"),
      Row(               "pk2",               4,                  "v4",              "12",     d"2022-01-10",            null)
    )

    import DataFrameSCDType2Support.implicits._
    val actual = source.periodicSnapshotToSCD2(Seq("my_primary_key"), Seq("int_value", "string_value"), "business_date")

    assertEquals(actual, expected)
  }
}
