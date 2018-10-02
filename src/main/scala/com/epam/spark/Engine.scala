package com.epam.spark

import java.util

import org.apache.spark.sql.Row

class Engine {

  def summary(list: util.List[Row]): Long = {
    var sum: Long = 0

    list.forEach(obj => sum = sum + Integer.valueOf(obj.toString()
      .replace("[", "")
      .replace("]",""))
    )

    return sum
  }

  /*def createMap():Map[String,Int] = {
    val map = new util.HashMap[String, Int]

    map.put()

  }*/

}
