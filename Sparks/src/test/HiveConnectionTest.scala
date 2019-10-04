package test
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
object HiveConnectionTest {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("app").master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://192.168.44.155:9083").enableHiveSupport()
      .getOrCreate()
     // val samplehive=spark.sql("show databases")
     //val schema= spark.sql("use default")
     val tables=spark.sql("select * from customers limit 5")
     tables.show()
    spark.stop()

  }
}