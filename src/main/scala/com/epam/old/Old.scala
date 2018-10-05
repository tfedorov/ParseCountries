package com.epam.old

import java.util
import org.apache.spark.sql.Row
import scala.collection.mutable

class Old {
  def sumToMap(list: util.List[Row]): mutable.HashMap[String, Double] = {
    val map = new mutable.HashMap[String, Double]

    list.forEach(obj => {
      val key: String = obj.getString(0)
      val doubleValue: Double = toDouble(obj.getString(1))
      map.put(
        key,
        if (map.contains(key)) map(key) + doubleValue else doubleValue
      )
    })

    return map


  }


  private def toDouble(value: String): Double = {
    return value
      .replace(",", ".")
      .toDouble
  }

}
