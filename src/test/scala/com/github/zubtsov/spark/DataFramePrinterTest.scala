package com.github.zubtsov.spark

import org.apache.spark.sql.Row

class DataFramePrinterTest extends SparkFunSuite {
  test("A generated_unique_id column should be added with unique values starting from offset") {
    import com.github.zubtsov.spark.SparkSessionCommons.implicits._
    val data = spark.createDataFrame(
      "short_col short, int_col int, long_col long, float_col float, double_col double, date_col date, timestamp_col timestamp, boolean_col boolean, string_col string, decimal_col decimal(18,4)",
      Row(Short.MinValue, Int.MinValue, Long.MinValue, Float.MinValue, Double.MinValue, new java.sql.Date(0L), new java.sql.Timestamp(0L), true, "short string", BigDecimal(-12345.12345)),
      Row(0.asInstanceOf[Short], 0, 0L, 0.0f, 0.0, new java.sql.Date(1L), new java.sql.Timestamp(1L), true, "string", BigDecimal(0.00001)),
      Row(null, null, null, null, null, null, null, null, null, null),
      Row(Short.MaxValue, Int.MaxValue, Long.MaxValue, Float.MaxValue, Double.MaxValue, new java.sql.Date(Long.MaxValue / 40000), new java.sql.Timestamp(Long.MaxValue / 40000), false, "_____long string____", BigDecimal(12345.12345))
    )

    import com.github.zubtsov.spark.DataFramePrinter.implicits._
    val actual = data.toScalaCode()
    println(actual)

    import com.github.zubtsov.spark.Literals.implicits._
    val printed = spark.createDataFrame(
      "         `short_col` SMALLINT, `int_col` INT,     `long_col` BIGINT, `float_col` FLOAT,     `double_col` DOUBLE, `date_col` DATE,  `timestamp_col` TIMESTAMP, `boolean_col` BOOLEAN,    `string_col` STRING, `decimal_col` DECIMAL(18,4)",
      Row(-32768.asInstanceOf[Short],   -2147483648, -9223372036854775808L,    -3.4028235E38f, -1.7976931348623157E308,   d"1970-01-01",   t"1970-01-01 03:00:00.0",                  true,         "short string",     BigDecimal(-12345.1235)),
      Row(     0.asInstanceOf[Short],             0,                    0L,              0.0f,                     0.0,   d"1970-01-01", t"1970-01-01 03:00:00.001",                  true,               "string",          BigDecimal(0.0000)),
      Row(                      null,          null,                  null,              null,                    null,            null,                       null,                  null,                   null,                        null),
      Row( 32767.asInstanceOf[Short],    2147483647,  9223372036854775807L,     3.4028235E38f,  1.7976931348623157E308,   d"9276-12-03", t"9276-12-03 21:42:01.369",                 false, "_____long string____",      BigDecimal(12345.1235))
    )
    printed.show(false)
  }
}
