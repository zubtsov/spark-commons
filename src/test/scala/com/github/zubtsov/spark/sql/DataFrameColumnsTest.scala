package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.SparkSessionCommons.implicits._
import org.apache.spark.sql.types._

class DataFrameColumnsTest extends SparkFunSuite {
  test("Test columns by type selectors") {
    val source = spark.emptyDataFrame2(new StructType()
      .add("col1", DoubleType)
      .add("col2", FloatType)
      .add("col3", ByteType)
      .add("col4", IntegerType)
      .add("col5", LongType)
      .add("col6", ShortType)
      .add("col7", DecimalType(18, 4))
      .add("col8", StringType)
      .add("col9", BinaryType)
      .add("col10", BooleanType)
      .add("col11", DateType)
      .add("col12", TimestampType)
      .add("col13", CalendarIntervalType)
      .add("col14", NullType)
    )

    import DataFrameColumns.implicits._
    assertResult(Seq("col1"))(source.doubleColumns)
    assertResult(Seq("col2"))(source.floatColumns)
    assertResult(Seq("col3"))(source.byteColumns)
    assertResult(Seq("col4"))(source.integerColumns)
    assertResult(Seq("col5"))(source.longColumns)
    assertResult(Seq("col6"))(source.shortColumns)
    assertResult(Seq("col7"))(source.decimalColumns)
    assertResult(Seq("col8"))(source.stringColumns)
    assertResult(Seq("col9"))(source.binaryColumns)
    assertResult(Seq("col10"))(source.booleanColumns)
    assertResult(Seq("col11"))(source.dateColumns)
    assertResult(Seq("col12"))(source.timestampColumns)
    assertResult(Seq("col13"))(source.calendarIntervalColumns)
    assertResult(Seq("col14"))(source.nullColumns)

    assertResult(Seq("col1", "col2", "col3", "col4", "col5", "col6", "col7"))(source.numericColumns)
  }
}
