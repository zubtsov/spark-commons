package com.github.zubtsov.spark

import java.sql.{Date, Timestamp}

object Literals {
  implicit class DateColumnInterpolation(private val sc: StringContext) extends AnyVal {
    def d(args: Any*): java.sql.Date = Date.valueOf(sc.s(args: _*))
  }

  implicit class TimestampColumnInterpolation(private val sc: StringContext) extends AnyVal {
    def t(args: Any*): java.sql.Timestamp = Timestamp.valueOf(sc.s(args: _*))
  }
}
