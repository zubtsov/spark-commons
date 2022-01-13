package com.github.zubtsov.spark.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lower, trim}
import org.apache.spark.sql.types.StringType

package object column {
  private[column] def transformString(t: Column => Column)(c: Column): Column =
    if (c.expr.resolved) {
      c.expr.dataType match {
        case StringType => t(c)
        case _          => c
      }
    } else {
      t(c) //WARN: conversion to the string type may happen here
    }
}
