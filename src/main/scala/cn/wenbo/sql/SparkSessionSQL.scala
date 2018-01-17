package scala.wenbo.sql

import org.apache.spark.sql.SparkSession

/**
  * spark2.0之后使用sparksession的方式使用sql
  */
object SparkSessionSQL extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("sqlTest")
    .getOrCreate()
  val jsonDF = spark.read.json("file:\\F:\\test\\data\\sparksql.json")
  jsonDF.select("name", "age").show()
  //jsonDF.select(df("name"), df())
  import spark.implicits._
  jsonDF.select($"name", $"age" + 1).show()
  // 创建临时视图
  jsonDF.createTempView("people")
  val sqlDF = spark.sql("select * from people")
  import spark.implicits._
  // 创建全局视图，生命周期直到spark程序退出才退出
  jsonDF.createGlobalTempView("student")
  val stuDF = spark.newSession().sql("select * from global_tmp.student")
  stuDF.show()
}
