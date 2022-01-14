package com.github.zubtsov.spark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Utility class for printing [[org.apache.spark.sql.DataFrame]].
 * Ideally, it should be replaced with some Scala formatter + rules, but it seems very complicated (if possible).
 */
object DataFramePrinter {
  object implicits {
    implicit class DataFrameAsString(df: DataFrame) {
      def toScalaCode(): String = {
        DataFramePrinter.toScalaCode(df)
      }
    }
  }

  /**
   * @param df a table with the data
   * @return a piece of Scala code for creating the passed table
   */
  def toScalaCode(df: DataFrame): String = {
    val rows = df.collect()
    val columnDefinitions: Seq[String] = df.schema.map(_.toDDL)
    val formattedRowsValues: Seq[Seq[String]] = rows.map(getRowAsSeq)
    val maxLengths: Seq[Int] = (formattedRowsValues :+ columnDefinitions)
      .map(s => s.map(_.length))
      .reduce((s1, s2) => {
        s1.zip(s2).map(t => Math.max(t._1, t._2))
      })
    val formattedRows = formattedRowsValues.map(vals => RowStart + vals.zipWithIndex.map(t => {
      val paddingLength = maxLengths(t._2)
      String.format("%" + paddingLength + "s", t._1)
    }).mkString(ColumnSeparator) + RowEnd).mkString(",\n")

    val formattedHeader = columnDefinitions.zipWithIndex.map(t => {
      val paddingLength = maxLengths(t._2) + (if (t._2 == 0) RowStart.length - 1 else 0)
      String.format("%" + paddingLength + "s", t._1)
    }).mkString(ColumnSeparator)
    Header + "\n" +
      " " * RowStart.length + "\"" + formattedHeader + "\",\n" +
      formattedRows + "\n" + Footer
  }

  /**
   * @param row the row with data
   * @return a piece of Scala code for creating the passed row
   */
  def toScalaCode(row: Row): String = {
    val rowContent = getRowAsSeq(row).mkString(ColumnSeparator)
    RowStart + rowContent + RowEnd
  }

  private def getRowAsSeq(row: Row): Seq[String] = {
    row.schema.zipWithIndex.map(t => {
      val sf = t._1
      val ind = t._2
      val value = row.get(ind)
      if (value == null) {
        "null"
      } else {
        sf.dataType match {
          case DecimalType() => s"BigDecimal($value)"
          case LongType => s"${value}L"
          case DateType => "d\"" + s"$value" + "\""
          case TimestampType => "t\"" + s"$value" + "\""
          case StringType => "\"" + value + "\""
          case FloatType => s"${value}f"
          case ShortType => s"${value}.asInstanceOf[Short]"
          case _ => String.valueOf(value)
        }
      }
    })
  }

  private val Header = "SparkSession.active.createDataFrame("
  private val RowStart = "Row("
  private val RowEnd = ")"
  private val Footer = ")"
  private val ColumnSeparator = ", "
}
