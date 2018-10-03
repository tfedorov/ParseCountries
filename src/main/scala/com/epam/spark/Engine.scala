package com.epam.spark

import java.util

import org.apache.spark.sql.Row

import scala.collection.mutable

class Engine {

  def summary(list: util.List[Row]): Long = {
    var sum: Long = 0

    list.forEach(obj => sum += obj.getInt(0))

    return sum
  }

  def sumToMap(list: util.List[Row]): mutable.HashMap[String, Double] = {
    val map = new mutable.HashMap[String, Double]

    list.forEach(obj => {
      val key: String = obj.getString(0).trim()

      val doubleValue: Double = toDouble(obj.getString(1))
      val population: Double = if (map.contains(key)) map(key) + doubleValue else doubleValue

      map.put(
        key,
        population)
    })

    return map
  }

  private def toDouble(value: String): Double = {
    return value
      .replace(",", ".")
      .toDouble
  }

}
