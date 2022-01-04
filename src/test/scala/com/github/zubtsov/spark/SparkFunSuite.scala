package com.github.zubtsov.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

//todo: share it as a part of a library
class SparkFunSuite extends AnyFunSuite {
  protected val ss = SparkSession
    .builder()
    .master("local[*]")

  protected val spark = ss
    .getOrCreate()
}
