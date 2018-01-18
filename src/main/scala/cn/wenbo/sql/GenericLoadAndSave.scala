package cn.wenbo.sql

import org.apache.spark.sql.SparkSession

object GenericLoadAndSave extends App{
  // 1. spark session
  val spark = SparkSession.builder()
    .appName("generric load and save")
    .master("local")
    .getOrCreate()
  // 2. 从hdfs中读取parquet文件
  val sparkDF = spark.read.load("hdfs://192.168.87.6:9000/spark/parquet/cube")
  // 3. 导入冲突依赖，下面要对DataFrame进行处理
  import spark.implicits._
  // 4. 对字段操作完注意重新修改别名，hdfs里面不支持 别名+1 这种操作操作符的存储
  val sparkDF1 = sparkDF.select(($"value" + 1).alias("value1"), ($"cube" + 1).alias("cube1"))
  // 5. 写入hdfs
  sparkDF1.write.save("hdfs://192.168.87.6:9000/spark/parquet/cubeplus1")
  val sparkDF3 = spark.read.load("hdfs://192.168.87.6:9000/spark/parquet/cubeplus1")
  sparkDF3.show()
}
