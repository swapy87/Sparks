package test

import org.apache.spark.SparkContext
import org.apache.log4j._
/*
 * flatmap can have one or multiple output
 */
object TotalSpentByCustomer {
  
  def recordParser(x:String) =
  {
    val fields =x.split(",")
    (fields(0).toInt,fields(2).toFloat)
    
  }
  
  
  def main(args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc =new SparkContext("local[2]","Swapy")
    
    val data = sc.textFile("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/TotalSpentByCustomer/customer-orders.csv").map(recordParser)
    val temp=data.reduceByKey((v1,v2)=>v1+v2)//(_+_)
    val flt=temp
    .filter(_._1%2==0)//iterate all the input _first and then after(.) then take second value
    .sortBy(_._2,true)
    //.collect()//wait for collection and then display
    .take(5)
    flt.foreach(println)//foreach is the only action that can called on other action
   // println(flt)
    
  }
}