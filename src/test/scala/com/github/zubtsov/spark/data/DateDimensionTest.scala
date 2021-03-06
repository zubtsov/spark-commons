package com.github.zubtsov.spark.data

import com.github.zubtsov.spark.SparkFunSuite
import org.apache.spark.sql.functions._

class DateDimensionTest extends SparkFunSuite {
  test("Date dimension simple test") {
    import spark.implicits._

    val result = new DateDimension().produce("2021-01-01", "2022-01-01")
    val expectedNumberOfRows = 365
    assertResult(expectedNumberOfRows)(result.count())
    assertResult(expectedNumberOfRows)(result.select("full_date").distinct().count())
    assertResult(expectedNumberOfRows)(result.select("date_key").distinct().count())
    assertResult(20210101)(result.select(min("date_key")).as[Int].first())
    assertResult(20211231)(result.select(max("date_key")).as[Int].first())
    assertResult(new java.sql.Date(1609448400000L))(result.select(min("full_date")).as[java.sql.Date].first())
    assertResult(new java.sql.Date(1640898000000L))(result.select(max("full_date")).as[java.sql.Date].first())
  }

  test("Date dimension SQL test") {
    import spark.implicits._
    val query = scala.io.Source.fromResource("date_dimension.sql").mkString

    val result = spark.sql(query)

    val expectedNumberOfRows = 365
    assertResult(expectedNumberOfRows)(result.count())
    assertResult(expectedNumberOfRows)(result.select("full_date").distinct().count())
    assertResult(expectedNumberOfRows)(result.select("date_key").distinct().count())
    assertResult(20210101)(result.select(min("date_key")).as[Int].first())
    assertResult(20211231)(result.select(max("date_key")).as[Int].first())
    assertResult(new java.sql.Date(1609448400000L))(result.select(min("full_date")).as[java.sql.Date].first())
    assertResult(new java.sql.Date(1640898000000L))(result.select(max("full_date")).as[java.sql.Date].first())
  }

  test("Date dimension Scala & SQL implementations are the same") {
    val query = scala.io.Source.fromResource("date_dimension.sql").mkString
    val sqlResult = spark.sql(query)
    val scalaResult = new DateDimension().produce("2021-01-01", "2022-01-01")

    import com.github.zubtsov.spark.DataFrameComparison._
    assertEqualData(sqlResult, scalaResult)
  }
}
