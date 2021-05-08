package com.github.zubtsov.spark.enums

//todo: override to string so it can be passed to standard join method
//todo: define apply method so it's possible to construct it from string
object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, Cross, Outer, LeftOuter, RightOuter, LeftSemi, RightSemi, Anti, LeftAnti, RightAnti = Value
}
