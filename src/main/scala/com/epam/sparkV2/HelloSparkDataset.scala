//package com.epam.sparkV2
//
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//
//import scala.reflect.internal.util.Statistics.Quantity
//
//object HelloSparkDataset {
//  def main(args: Array[String]): Unit = {
//    val spark =SparkSession.builder()
//      .master("local")
//      .appName("simpleReading")
//      .getOrCreate()
//    import spark.implicits._
//    case class Record(country:String, region:String, population: Int, area: Int, popDens:Int)
//
//     val frame: DataFrame = spark.read.option("header", "true")
//       .option("charset", "UTF8").csv("src\\main\\scala\\resourses\\countries_of_the_world.csv")
//    val lines= frame.as[Record]
//    val pop: DataFrame = lines.select("Population").filter("\\d")
//    println(pop)
//
//
//  }
//
//}
