package scala.wenbo.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameSQL {
  def main(args: Array[String]): Unit = {
    println("......")
    val conf = new SparkConf()
      .setAppName("spark sql")
      //.setMaster("local")
      //.setMaster("spark://192.168.87.6")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //val df = sqlContext.read.json("file:\\F:\\test\\data\\sparksql.json")
    val df = sqlContext.read.json("file:/tmp/test/sparksql.json")
    println("--------------------打印全表")
    df.show()
    println("--------------------打印表结构")
    df.printSchema()
    println("--------------------打印一列")
    df.select("name").show()
    println("--------------------打印一列，年龄+1")
    df.select(df("age") + 1, df("name")).show()
    println("--------------------过滤")
    df.filter(df("age") > 29).show()
    println("--------------------分组")
  }
}
