package com.epam.sparkV2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloSparkRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    var sum: Long = 0
    val distFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")
    println("                      ")
    val value: RDD[(String, Int)] = distFile.map(obj => (obj, 1))
//    println(value.re)
    println("                      ")

  }

  private def test(str: String):String = {
    print(1 + str)
    return str
  }

}
