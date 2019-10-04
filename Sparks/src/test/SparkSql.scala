package test

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

/*
 * 1)load data in RDD
 * 2)define schema
 * 3)
 * 
 */
object SparkSql {
  
  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    
    val spark= SparkSession.builder().appName("app").master("local[2]")
    .config("spark.sql.warehouse.dir","file:///c:/temp")
    .getOrCreate()
    val data =spark.sparkContext.textFile("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/TotalSpentByCustomer/customer-orders.csv")
    val rec= data.map(recordParser)
    rec.foreach(println)
    import spark.implicits._
    val myDF= rec.toDF()
   print("*************************SCHEMA******************************\n")
    myDF.printSchema()
    print("**************************DISPLAY****************************\n")
    myDF.show(30)
    
    myDF.createOrReplaceTempView("view_user")
    val data1= spark.sql("select * from view_user where id>50")
    print("**********************show data from view********************\n")
    data1.show(30)
    
  }
  
  case class Users(ID:Int, amount:Double)
  
  def recordParser(x:String) :Users=
  {
    val fields=x.split(",")
    val user_data :Users=Users(fields(0).toInt,fields(2).toFloat)
    return user_data
  }
  
}