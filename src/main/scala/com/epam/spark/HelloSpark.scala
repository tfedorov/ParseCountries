package com.epam.spark

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("simpleReading").master("local[*]").getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src\\main\\scala\\resourses\\countries_of_the_world.csv")

    val engine = new Engine()

    //Show to console sum of population
    val population = df.select("Population")
    println("Sum of population: " + engine.summary(population.collectAsList()))


    //Show Population Density at region
    val regionAndPopulation = df.select("Region", "PopDensity")
    engine.sumToMap(regionAndPopulation.collectAsList())
      .foreach(obj => println(obj))
  }

}
