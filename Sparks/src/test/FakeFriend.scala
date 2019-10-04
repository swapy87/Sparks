package test


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import shapeless.ops.nat.ToInt

object FakeFriend {
  
  
  def main(args:Array[String])
  {
      Logger.getLogger("org").setLevel(Level.OFF)
       val spark= SparkSession.builder().appName("app").master("local[2]")
    .config("spark.sql.warehouse.dir","file:///c:/temp")
    .getOrCreate()
    
    val data =spark.sparkContext.textFile("C:/Users/Swapnil/Desktop/BigData/Module-5/Spark/SparkSql/fakefriends.csv").map(recordData)
      data.foreach(println)
      
      import spark.implicits._
      val myDF=data.toDF()
      myDF.printSchema()
      myDF.createOrReplaceTempView("fakefriend")
      val fakefriend=spark.sql("select * from fakefriend where age<19")
      fakefriend.show()
      spark.close()
    
  }
  
  case class People(name:String , age:Int) 
  def recordData(x:String) :People=
  {
    val fileds=x.split(",")
    val pep:People=People(fileds(1).toString(),fileds(2).toInt)
    return pep
  }
}