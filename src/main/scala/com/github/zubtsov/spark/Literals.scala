package com.github.zubtsov.spark

import java.sql.{Date, Timestamp}

/**
 * Useful string interpolations for defining [[java.sql.Date]] and [[java.sql.Timestamp]] literals e.g. in tests
 */
object Literals {
  object implicits {
    implicit class DateColumnInterpolation(private val sc: StringContext) extends AnyVal {
      def d(args: Any*): java.sql.Date = Literals.d(sc.s(args: _*))
    }

    implicit class TimestampColumnInterpolation(private val sc: StringContext) extends AnyVal {
      def t(args: Any*): java.sql.Timestamp = Literals.t(sc.s(args: _*))
    }
  }

  def d(date: String): java.sql.Date = Date.valueOf(date)

  def t(timestamp: String): java.sql.Timestamp = Timestamp.valueOf(timestamp)
}
