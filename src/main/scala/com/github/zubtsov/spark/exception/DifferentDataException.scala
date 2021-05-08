package com.github.zubtsov.spark.exception

import com.github.zubtsov.spark.DataFrameComparison.DataDifference

final case class DifferentDataException(dataDifference: DataDifference) extends Exception
