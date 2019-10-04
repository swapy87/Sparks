package test


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.SQLContext

object SparkSQLNew {
  def main(args:Array[String])
  {

  Logger.getLogger("org").setLevel(Level.OFF)
    
    val spark= SparkSession.builder().appName("app").master("local[2]")
    .config("spark.sql.warehouse.dir","file:///c:/temp")
    .getOrCreate()
    
    val empFile=spark.sparkContext.textFile("C:/Users/Swapnil/Desktop/BigData/Module-2/datasets/emp_csv")
    
   println( "COUNT  "+empFile.count())
   
   
   //val sqlContext= new SQLContext(spark.)
  }
}