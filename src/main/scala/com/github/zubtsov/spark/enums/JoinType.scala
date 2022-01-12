package com.github.zubtsov.spark.enums

object JoinType extends Enumeration {
  type JoinType = String
  val Inner = "inner"

  val Cross = "cross"

  val Outer = "outer"
  val Full = "full"
  val FullOuter = "full_outer"

  val Left = "left"
  val LeftOuter = "left_outer"
  val Right = "right"
  val RightOuter = "right_outer"

  val Semi = "semi"
  val LeftSemi = "left_semi"

  val Anti = "anti"
  val LeftAnti = "left_anti"
}
