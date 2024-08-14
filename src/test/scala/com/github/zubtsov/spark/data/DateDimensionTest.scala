package com.github.zubtsov.spark.data

import com.github.zubtsov.spark.SparkFunSuite
import org.apache.spark.sql.functions._

class DateDimensionTest extends SparkFunSuite {
  test("Date dimension simple test") {
    import spark.implicits._

    val startDate = java.sql.Date.valueOf("1970-01-01")
    val endDate = java.sql.Date.valueOf("2023-12-31")

    val result = new DateDimension().produce("1970-01-01", "2024-01-01")
    val expectedNumberOfRows = 19723
    assertResult(expectedNumberOfRows)(result.count())
    assertResult(expectedNumberOfRows)(result.select("full_date").distinct().count())
    assertResult(expectedNumberOfRows)(result.select("date_key").distinct().count())
    assertResult(19700101)(result.select(min("date_key")).as[Int].first())
    assertResult(20231231)(result.select(max("date_key")).as[Int].first())
    assertResult(startDate)(result.select(min("full_date")).as[java.sql.Date].first())
    assertResult(endDate)(result.select(max("full_date")).as[java.sql.Date].first())
  }

  test("Date dimension SQL test") {
    import spark.implicits._
    val query = scala.io.Source.fromResource("date_dimension.sql").mkString

    val startDate = java.sql.Date.valueOf("1970-01-01")
    val excludedEndDate = java.sql.Date.valueOf("2024-01-01")
    val endDate = java.sql.Date.valueOf("2023-12-31")

    val result = spark.sql(query, Map(
      "startDate" -> startDate,
      "endDate" -> excludedEndDate
    ))

    val expectedNumberOfRows = 19723
    assertResult(expectedNumberOfRows)(result.count())
    assertResult(expectedNumberOfRows)(result.select("full_date").distinct().count())
    assertResult(expectedNumberOfRows)(result.select("date_key").distinct().count())
    assertResult(19700101)(result.select(min("date_key")).as[Int].first())
    assertResult(20231231)(result.select(max("date_key")).as[Int].first())
    assertResult(startDate)(result.select(min("full_date")).as[java.sql.Date].first())
    assertResult(endDate)(result.select(max("full_date")).as[java.sql.Date].first())
  }

  test("Date dimension Scala & SQL implementations are the same") {
    val startDate = java.sql.Date.valueOf("1970-01-01")
    val excludedEndDate = java.sql.Date.valueOf("2024-01-01")

    val query = scala.io.Source.fromResource("date_dimension.sql").mkString
    val sqlResult = spark.sql(query, Map(
      "startDate" -> startDate,
      "endDate" -> excludedEndDate
    ))
    val scalaResult = new DateDimension().produce("1970-01-01", "2024-01-01")

    import com.github.zubtsov.spark.DataFrameComparison._
    assertEqualData(sqlResult, scalaResult)
  }
}
