package com.github.zubtsov.spark

import org.apache.spark.sql.SparkSession

package object zubtsov {
  private val defaultCaseSensPropName = "com.github.zubtsov.spark.caseSensitive"

  protected[spark] def defaultCaseSensitivity: Boolean = {
    SparkSession.getActiveSession.exists(_.conf.getOption(defaultCaseSensPropName).isDefined)
  }

  protected[spark] def areStringsEqual(caseSensitive: Boolean)(s1: String, s2: String): Boolean = {
    if (caseSensitive) s1.equals(s2)
    else s1.equalsIgnoreCase(s2)
  }
}
