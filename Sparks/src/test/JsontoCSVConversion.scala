package test

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.log4j._
object JsontoCSVConversion {
  def main(agrs : Array[String]){
    Logger.getLogger("org").setLevel(Level.OFF)
      val spark=SparkSession
                 .builder()
                 .appName("JsonToCSV")
                 .master("local[*]")
                 .config("spark.sql.warehouse.dir", "file:///c:/temp")
                 .getOrCreate()
               
    //val jsonFile= spark.read.json("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/JSONParser/customers.json")
    //customer_nested_tags1.json
       val jsonFile= spark.read.json("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/JSONParser/customer_nested_tags1.json")
                 val falttenJson=jsonFile.select("info.cuSTNo","iNFo.firstname")
    
    //its case insensitive
    falttenJson.show(50)
    falttenJson.write.mode(SaveMode.Append).csv("C:/Users/Swapnil/Desktop/lz")
    
  }
  
}