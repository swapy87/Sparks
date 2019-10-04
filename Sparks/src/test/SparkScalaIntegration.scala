package test

import org.apache.spark.SparkContext
import org.apache.log4j._

/*
 * we cant call action on action
 */
object SparkScalaIntegration {
  
  def main (args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc = new SparkContext("local[*]","SwapnilJob")
    val data =sc.textFile("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/WordCount/book.txt").flatMap(f=>f.split(" ")).countByValue()
    val data1 =sc.textFile("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/WordCount/book.txt").flatMap(f=>f.split(" ")).count()
    //val output=data.collect()
   // val wc = data.countByValue()   // terminal operator or end in sparx called action
    data.foreach(println)
    println("********************************")
    println("count  "+data1)
    //print("************************************")
    //wc.foreach(println)
  }
  
}