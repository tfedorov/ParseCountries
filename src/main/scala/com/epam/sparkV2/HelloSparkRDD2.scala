package com.epam.sparkV2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD



object HelloSparkRDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val distFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")

    val value: RDD[String] = distFile.map(_.split(",")(2))
      .filter(str => !str.contains("Population"))
   val rdd = value.map(_.toLong)
    println("Population total: " + rdd.take(rdd.count().toInt).sum)

    val unit: RDD[String] = distFile.map(elem => elem.split(",\""))
      .collect{case x if (x.length>1) => x(1)
        .stripSuffix("\"")}
    val rdd2 = unit.map(_.replace(",",".").toDouble)
    println("Pop. Density total: " + rdd2.take(rdd.count().toInt).sum)
  }

}
