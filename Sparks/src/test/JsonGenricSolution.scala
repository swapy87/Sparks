package test


import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.log4j._
import java.util.Properties
import java.io.FileInputStream

object JsonGenricSolution {
  
  def main (args : Array[String])
  {
     Logger.getLogger("org").setLevel(Level.OFF)
     
     val spark= SparkSession
                 .builder()
                 .appName("JSONGenric")
                 .master("local[*]")
                 .config("spark.sql.warehouse.dir","file:///C:/temp")
                 .getOrCreate()
                 
      val jsonfile=spark.read.json(args(0))
      val prop : Properties=new Properties
      prop.load(new FileInputStream(args(1)))
      
      val jsonColoum=prop.getProperty(args(2))

      println(jsonColoum)
      
      val json_parse=jsonfile.selectExpr(jsonColoum.split(","):_*).repartition(1)// repartion will clum from all the partition and club it to the specified number
      //:_* --> this indicat repetation on cloum and (*) on N number of coloum
      //repartition to club in one
      
      println(json_parse)
  }
}