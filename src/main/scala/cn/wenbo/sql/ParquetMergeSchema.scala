package scala.wenbo.sql

import org.apache.spark.sql.{Row, SparkSession}

object ParquetMergeSchema extends App {
  // 1. 创建sparksession
  val spark = SparkSession.builder()
    // 定义app名称，后期sparkUI会用来区分
    .appName("schema_merge")
    // 定义运行spark的模式为本地模式
    .master("local")
    // 拿到或者创建sparkSession
    .getOrCreate()
  // 2. 使用序列创建RDD，经过一次变换，转成DataFrame，写入hdfs
  case class square(value: Int, square: Int)
  import spark.implicits._
//  val df1 = spark.sparkContext.makeRDD(1 to 5).map( x => square(x, x * x)).toDF
//  df1.write.parquet("hdfs://192.168.87.6:9000/spark/parquet/square")
//  case class cube(value: Int, cube: Int)
//  val df2 = spark.sparkContext.makeRDD(6 to 10).map( x => cube(x, x * x * x)).toDF
//  df2.write.parquet("hdfs://192.168.87.6:9000/spark/parquet/cube")
  //3.
  val mergeDF = spark.read.option("mergeSchema", true).parquet("hdfs://192.168.87.6:9000/spark/parquet/cube")
  mergeDF.printSchema()
  mergeDF.show()
}
