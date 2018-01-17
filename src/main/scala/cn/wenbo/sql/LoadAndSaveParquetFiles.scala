package scala.wenbo.sql
import org.apache.spark.sql.{Row, SparkSession}
object LoadAndSaveParquetFiles extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("load and save parquets")
    .getOrCreate()
  val usersDF = spark.read.load("file:\\F:\\test\\data\\users.parquet")
  usersDF.show()
  /**
    * paquet 文件写本地有问题
    */
  //usersDF.select("name").write.save("names.parquet")
  //val namesDF = spark.read.load("file:\\F:\\test\\data\\names.parquet")
  //namesDF.show()
}
