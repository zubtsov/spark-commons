package com.github.zubtsov.spark.sql

import com.github.zubtsov.spark.defaultCaseSensitivity
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object DataFrameColumns {

  object implicits {
    implicit class DataFrameColumnsImpl(df: DataFrame) {
      private def any[A](predicates: (A => Boolean)*): A => Boolean =
        a => predicates.exists(pred => pred(a))

      private val isDoubleColumn: (StructField) => Boolean = sf => sf.dataType == DoubleType
      private val isFloatColumn: (StructField) => Boolean = sf => sf.dataType == FloatType
      private val isByteColumn: (StructField) => Boolean = sf => sf.dataType == ByteType
      private val isIntColumn: (StructField) => Boolean = sf => sf.dataType == IntegerType
      private val isLongColumn: (StructField) => Boolean = sf => sf.dataType == LongType
      private val isShortColumn: (StructField) => Boolean = sf => sf.dataType == ShortType
      private val isDecimalColumn: (StructField) => Boolean = sf =>
        sf.dataType match {
          case DecimalType() => true
          case _ => false
        }

      def stringColumns: Seq[String] = df.schema.filter(_.dataType == StringType).map(_.name)

      def binaryColumns: Seq[String] = df.schema.filter(_.dataType == BinaryType).map(_.name)

      def booleanColumns: Seq[String] = df.schema.filter(_.dataType == BooleanType).map(_.name)

      def dateColumns: Seq[String] = df.schema.filter(_.dataType == DateType).map(_.name)

      def timestampColumns: Seq[String] = df.schema.filter(_.dataType == TimestampType).map(_.name)

      def calendarIntervalColumns: Seq[String] = df.schema.filter(_.dataType == CalendarIntervalType).map(_.name)

      def dateTimestampColumns: Seq[String] =
        df.schema.filter(sf => sf.dataType == DateType || sf.dataType == TimestampType).map(_.name)

      def doubleColumns: Seq[String] = df.schema.filter(isDoubleColumn).map(_.name)

      def floatColumns: Seq[String] = df.schema.filter(isFloatColumn).map(_.name)

      def byteColumns: Seq[String] = df.schema.filter(isByteColumn).map(_.name)

      def integerColumns: Seq[String] = df.schema.filter(isIntColumn).map(_.name)

      def longColumns: Seq[String] = df.schema.filter(isLongColumn).map(_.name)

      def shortColumns: Seq[String] = df.schema.filter(isShortColumn).map(_.name)

      def nullColumns: Seq[String] = df.schema.filter(_.dataType == NullType).map(_.name)

      def decimalColumns: Seq[String] = df.schema.filter(isDecimalColumn).map(_.name)

      def numericColumns: Seq[String] =
        df.schema
          .filter(
            any(isDoubleColumn, isFloatColumn, isByteColumn, isIntColumn, isLongColumn, isShortColumn, isDecimalColumn))
          .map(_.name)

      def columnsMatching(colNameRegex: String): Seq[String] = df.columns.filter(_.matches(colNameRegex))

      def columnsStartingWith(colNamePrefix: String, caseSensitive: Boolean = defaultCaseSensitivity): Seq[String] =
        df.columns.filter(cn => {
          if (caseSensitive) cn.startsWith(colNamePrefix) else cn.toLowerCase.startsWith(colNamePrefix.toLowerCase)
        })

      def columnsEndingWith(colNameSuffix: String, caseSensitive: Boolean = defaultCaseSensitivity): Seq[String] = df.columns.filter(cn => {
        if (caseSensitive) cn.endsWith(colNameSuffix) else cn.toLowerCase.endsWith(colNameSuffix.toLowerCase)
      })
    }
  }

}
