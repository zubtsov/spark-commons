package com.github.zubtsov.spark.enums

object UnionStrategy extends Enumeration {
  type UnionStrategy = Value
  val CommonColumns, AllColumns, LeftColumns, RightColumns = Value
}
