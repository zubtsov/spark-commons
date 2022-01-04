package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.SparkFunSuite
import org.apache.spark.sql.Encoders

class DataFrameWithIdTest extends SparkFunSuite {
  test("A generated_unique_id column should be added with unique and continuous values starting from specified offset 2") {
    val start = 0L
    val end = 10000L
    val size = end - start
    val source = spark.range(start,end, 1L, 33).toDF("some_int")

    import DataFrameWithId.implicits._
    import org.apache.spark.sql.functions.{max, min}

    val result = source.zipWithId(Long.MaxValue - size, "generated_unique_id")
    val maxValue = result.select(max("generated_unique_id")).as(Encoders.LONG).collect().head
    val minValue = result.select(min("generated_unique_id")).as(Encoders.LONG).collect().head
    val countOfValues = result.count()
    val countOfDistinctValues = result.distinct().count()

    assertResult(Long.MaxValue - size)(minValue)
    assertResult(Long.MaxValue - 1)(maxValue)
    assertResult(size)(countOfValues)
    assertResult(size)(countOfDistinctValues)
  }

  test("A generated_unique_id column should be added with unique and continuous values starting from specified offset") {
    val size = 10000
    val source = spark.range(size).toDF("some_int")

    import DataFrameWithId.implicits._
    import org.apache.spark.sql.functions.{max, min}

    val result = source.zipWithIndex(Long.MaxValue - size, "generated_unique_id")
    val maxValue = result.select(max("generated_unique_id")).as(Encoders.LONG).collect().head
    val minValue = result.select(min("generated_unique_id")).as(Encoders.LONG).collect().head
    val countOfValues = result.count()
    val countOfDistinctValues = result.distinct().count()

    assertResult(Long.MaxValue - size)(minValue)
    assertResult(Long.MaxValue - 1)(maxValue)
    assertResult(size)(countOfValues)
    assertResult(size)(countOfDistinctValues)
  }

  test("A generated_unique_id column should be added with unique values starting from the specified offset") {
    val size = 10000
    val offset = 1
    val source = spark.range(size).toDF("some_int")

    import DataFrameWithId.implicits._
    import org.apache.spark.sql.functions.min

    val result = source.zipWithIndex(offset, "generated_unique_id")
    val minValue = result.select(min("generated_unique_id")).as(Encoders.LONG).collect().head
    val countOfValues = result.count()
    val countOfDistinctValues = result.distinct().count()

    assertResult(offset)(minValue)
    assertResult(size)(countOfValues)
    assertResult(size)(countOfDistinctValues)
  }
}
