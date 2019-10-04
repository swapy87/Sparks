package test
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object NetworkWord {
  
  def main(args:Array[String])
  {
     Logger.getLogger("org").setLevel(Level.ERROR)
     val sparConf = new SparkConf().setAppName("StreamingApp").setMaster("local[*]")
     val ssc= new StreamingContext(sparConf,Seconds(10))
     val lines = ssc.socketTextStream(args(0), args(1).toInt,StorageLevel.MEMORY_AND_DISK)
     val words = lines.flatMap(v=> v.split(""))
     /*val cnt=words.count()
     cnt.print()*/
     lines.print()
     ssc.start()
     ssc.awaitTermination()
  }
  
  
}