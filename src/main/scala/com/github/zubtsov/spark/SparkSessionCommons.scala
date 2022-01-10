package com.github.zubtsov.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSessionCommons {

  object implicits {

    implicit class SparkSessionCommonsImpl(ss: SparkSession) { //TODO: utilize SparkSession object?
      /**
       * [[SparkSessionCommons.emptyDataFrame2]]
       */
      def emptyDataFrame2(schema: StructType): DataFrame = {
        SparkSessionCommons.emptyDataFrame2(schema)
      }
      /**
       * [[SparkSessionCommons.emptyDataFrame2]]
       */
      def emptyDataFrame2(schema: String): DataFrame = {
        SparkSessionCommons.emptyDataFrame2(schema)
      }

      def createDataFrame(schema: StructType, rows: Seq[Row]): DataFrame = {
        SparkSessionCommons.createDataFrame(schema, rows)
      }

      def createDataFrame(schema: StructType, rows: Seq[Row], numSlices: Int): DataFrame = {
        SparkSessionCommons.createDataFrame(schema, rows, numSlices)
      }

      def createDataFrame(schema: String, rows: Row*): DataFrame = {
        SparkSessionCommons.createDataFrame(StructType.fromDDL(schema), rows)
      }
    }
  }

  /** Returns an empty [[org.apache.spark.sql.DataFrame]] with the specified schema.
   *
   * @param schema the schema of an empty [[org.apache.spark.sql.DataFrame]].
   */
  def emptyDataFrame2(schema: StructType): DataFrame = {
    SparkSession.active.emptyDataFrame.select(
      schema.map(sf => lit(null).cast(sf.dataType).as(sf.name)): _*
    )
  }

  def emptyDataFrame2(schema: String): DataFrame = {
    emptyDataFrame2(StructType.fromDDL(schema))
  }

  def createDataFrame(schema: StructType, rows: Seq[Row]): DataFrame = {
    val rdd = SparkSession.active.sparkContext.parallelize(rows)
    SparkSession.active.createDataFrame(rdd, schema)
  }

  def createDataFrame(schema: StructType, rows: Seq[Row], numSlices: Int): DataFrame = {
    val rdd = SparkSession.active.sparkContext.parallelize(rows, numSlices)
    SparkSession.active.createDataFrame(rdd, schema)
  }

  def createDataFrame(schema: String, rows: Row*): DataFrame = {
    createDataFrame(StructType.fromDDL(schema), rows)
  }
}
