package com.epam.sparkV2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object HelloSparkRDDV2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("simpleReading").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val distFile: RDD[String] = sc.textFile("src\\main\\scala\\resourses\\countries_of_the_world.csv")
    val csvFile: RDD[Array[String]] = distFile.map(_.split(","))
    val populationIndex: Int = 2
    val regionIndex: Int = 1
    val popDensityIntegerIndex: Int = 4
    val popDensityFractionalIndex: Int = 5


    //Show to console sum of population
    val population: RDD[String] = csvFile.map(_(populationIndex)).filter(str => !str.contains("Population"))
    val popSum: RDD[Long] = population.map(_.toLong)
    println("Population total: " + popSum.sum.toLong)


    //Show Population Density at region
    val region: RDD[String] = csvFile.map(_(regionIndex)).filter(str => !str.contains("Region"))
    val popDensityInteger: RDD[String] = csvFile.map(_(3)).filter(str => !str.contains("Coastline (coast/area ratio)"))
    val popDensityFractional: RDD[String] = csvFile.map(_(popDensityFractionalIndex)).filter(str => !str.contains("Net migration"))

    println("                     ")
//    region.foreach(println)
    println("                     ")
    popDensityInteger.foreach(println)
    println("                     ")
//    popDensityFractional.foreach(println)
//    println("                     ")


//    val popDensity: RDD[String] = distFile.map(elem => elem.split(",\""|))
//      .collect { case x if (x.length > 1) => x(1)
//        .stripSuffix("\"")
//      }
//    val popDensityDouble: RDD[Double] = popDensity.map(_.replace(",", ".").toDouble)
//    println("Pop. Density total: " + popDensityDouble.sum)
  }

}
