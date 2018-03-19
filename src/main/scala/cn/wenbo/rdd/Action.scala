package cn.wenbo.rdd

import org.apache.spark.sql.SparkSession

object Action {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      /**
        * 1. 初始化配置
        */
      .appName("action")
      .master("local") // 本地单线程
      .getOrCreate()
    /**
      * 2. 加载数据
      */
    val lines = spark.sparkContext.textFile("/Applications/fluency/data/spark/student.txt")
    //val lines = spark.sparkContext.textFile("hdfs://localhost:900654auri4y4u9uu9q98ti;'0/spark/parquet/cubeplus1") // 读取hdfs中的数据
    // 创建rdd的另外一种方式
    val zoo = spark.sparkContext.parallelize(List("monkey", "panda", "donkey", "bear", "lion", "bear"))
    zoo.collect().foreach(println)
    val animals = zoo.flatMap(_.split(" "))
    zoo.persist()
    /**
      * rdd 常用的transformation操作
      */
    // 1. filter
    val bear = animals.filter(animal => animal == "bear")
    bear.collect().foreach(println)
    // 2. map flatMap
    // 3. distinct
    print("----------------------distinct操作")
    animals.distinct().collect().foreach(println)
    //

    /**
      * rdd 常用的action操作
      */
    val threeAnimals = animals.take(3).foreach(println)
    val allAnimals = animals.collect() // collect 获取rdd的所有数据，大数据量的rdd不能使用该操作
  }
}
