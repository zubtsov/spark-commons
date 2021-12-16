package com.github.zubtsov.spark.data

import com.github.zubtsov.spark.data.DateDimension._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Date
import java.text.{DateFormat, SimpleDateFormat}
import java.util.concurrent.TimeUnit

//TODO: add separate method with fiscal_month_of_year,fiscal_quarter,fiscal_year,fiscal_year_month,fiscal_year_qtr columns

/**
 * Utility class for producing date dimension tables
 * @param dateFormat the format in which start and end dates are specified
 */
class DateDimension(private val dateFormat: DateFormat = new SimpleDateFormat(ISO8601DateFormat)) {
  /**
   * Produces a standard date dimension
   * @param startIncluded the earliest date in the table (included)
   * @param endExcluded the latest date in the table (excluded)
   * @return the date dimension table
   */
  def produce(startIncluded: String = dateFormat.format(new java.util.Date(0)),
              endExcluded: String = dateFormat.format(new java.util.Date())): DataFrame = {

    val startDate = new Date(dateFormat.parse(startIncluded).getTime)
    val endDate = new Date(dateFormat.parse(endExcluded).getTime)
    val diffInDays = getDiffInDays(startDate, endDate)

    SparkSession.active.range(diffInDays)
      .withColumn(DateColName, date_add(lit(startDate), col("id").cast(IntegerType)))
      .withColumn("date_key", date_format(col(DateColName), "yyyyMMdd").cast(IntegerType))
      .withColumn("full_date_description", date_format(col(DateColName), "MMMM dd, yyyy"))

      .withColumn("date_iso_8601", date_format(col(DateColName), ISO8601DateFormat))

      .withColumn("day_of_week_short_name", date_format(col(DateColName), "EEE"))
      .withColumn("day_of_week_name", date_format(col(DateColName), "EEEE"))
      .withColumn("weekday_weekend",
        when(dayofweek(col(DateColName)).isin(1,7),
          lit("Weekend")
        ).otherwise(lit("Weekday"))
      )

      .withColumn("day_of_week", dayofweek(col(DateColName)))
      .withColumn("day_of_month", dayofmonth(col(DateColName)))
      .withColumn("day_of_year", dayofyear(col(DateColName)))
      .withColumn("week_of_year", weekofyear(col(DateColName)))

      .withColumn("last_day_of_month_date", last_day(col(DateColName)))
      .withColumn("is_last_day_of_month", col("last_day_of_month_date") === col(DateColName))

      .withColumn("month_name_short", date_format(col(DateColName), "MMM"))
      .withColumn("month_name", date_format(col(DateColName), "MMMM"))
      .withColumn("quarter_name", concat(lit("Q"), quarter(col(DateColName))))

      .withColumn("calendar_year", year(col(DateColName)))
      .withColumn("calendar_quarter", quarter(col(DateColName)))
      .withColumn("calendar_month", month(col(DateColName)))
      .withColumn("calendar_year_quarter", concat(col("calendar_year"), lit("-"), col("calendar_quarter")))
      .withColumn("calendar_year_month", concat(col("calendar_year"), lit("-"), lpad(col("calendar_month"), 2, "0")))
  }

  private def getDiffInDays(startDate: Date, endDate: Date): Long = {
    val diffInMilliseconds = Math.abs(startDate.getTime - endDate.getTime)
    TimeUnit.DAYS.convert(diffInMilliseconds, TimeUnit.MILLISECONDS)
  }
}

private object DateDimension {
  private val DateColName = "full_date"
  private val ISO8601DateFormat = "yyyy-MM-dd"
}