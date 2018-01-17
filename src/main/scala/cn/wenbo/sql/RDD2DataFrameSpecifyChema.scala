package scala.wenbo.sql

import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 在程序中定义schema
  */
object RDD2DataFrameSpecifyChema extends App {
  // 1. 创建sparksession
  val spark = SparkSession.builder()
    // 定义app名称，后期sparkUI会用来区分
    .appName("use_schema")
    // 定义运行spark的模式为本地模式
    .master("local")
    // 拿到或者创建sparkSession
    .getOrCreate()
  // 2. 创建元素格式为Row类型的RDD
  val rowRDD = spark.sparkContext.textFile("file:\\F:\\test\\data\\student.txt")
    // 通过条用sparkContext的textFile方法，将文本文件生成元素为String类型的RDD
    // 默认每一行的数据为一个元素
    // 对每个元素使用逗号进行拆分，拆分的结果为字符数组
    .map(line => line.split(","))
    // 将每个数组转换成Row类型的对象，注意数值类型的数据需要转换成数字类型
    .map(arr => Row(arr(0), arr(1).trim.toInt, arr(2)))
  // 3. 创建schema，类型为StructType
  val structType = StructType(
    // StructType的参数为StructField数组
    Array(
      // structField 的参数分别为 字段名称、字段类型、是否为空
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("sex", StringType, nullable = true)
    )
  )
  // 4. 使用sparkSession的createDataFrame方法创建DataFrame
  // 参数为，ROW类型的RDD 和 StructType对象
  val peopleDF = spark.createDataFrame(rowRDD, structType)
  peopleDF.show()
}
