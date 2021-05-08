package com.github.zubtsov.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSessionCommons {

  object implicits {
    implicit class SparkSessionCommons(ss: SparkSession) {
      /** Returns an empty [[org.apache.spark.sql.DataFrame]] with the specified schema.
       *
       * @param schema the schema of an empty [[org.apache.spark.sql.DataFrame]].
       */
      def emptyDataFrame2(schema: StructType): DataFrame = {
        ss.emptyDataFrame.select(
          schema.map(sf => lit(null).cast(sf.dataType).as(sf.name)): _*
        )
      }

      def emptyDataFrame2(schema: String): DataFrame = {
        emptyDataFrame2(StructType.fromDDL(schema))
      }

      def createDataFrame(schema: StructType, rows: Seq[Row]): DataFrame = {
        val rdd = ss.sparkContext.parallelize(rows)
        ss.createDataFrame(rdd, schema)
      }

      def createDataFrame(schema: StructType, rows: Seq[Row], numSlices: Int): DataFrame = {
        val rdd = ss.sparkContext.parallelize(rows, numSlices)
        ss.createDataFrame(rdd, schema)
      }

      def createDataFrame(schema: String, rows: Row*): DataFrame = {
        createDataFrame(StructType.fromDDL(schema), rows)
      }
    }
  }

}
