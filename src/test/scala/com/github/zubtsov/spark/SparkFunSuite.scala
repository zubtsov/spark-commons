package com.github.zubtsov.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.ParallelTestExecution

//todo: share it as a part of a library
class SparkFunSuite extends AnyFunSuite with ParallelTestExecution {
  protected val ss = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.driver.host", "localhost")
    .master("local[*]")

  protected val spark = ss
    .getOrCreate()
}
