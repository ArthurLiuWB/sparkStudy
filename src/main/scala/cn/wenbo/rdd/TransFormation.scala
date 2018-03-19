package cn.wenbo.rdd

import org.apache.spark.sql.SparkSession

object TransFormation {
  def main(args: Array[String]): Unit = {
    /**
      * spark 2.0 之前的版本
      * 进行初始化
      */
    //  val conf = new SparkConf()
    //    .setAppName("word count")
    //    .setMaster("local")
    //  val sc = new SparkContext(conf)
    /**
      * 创建SparkSession，2.0之后的新特性，
      * 完成了对SparkConf 和 SparkContext的封装
      */
    val spark = SparkSession.builder()
      /**
        * 1. 初始化配置
        */
      .appName("word count")
      .master("local") // 本地单线程
      //.master("local[2]") // 本地多线程，指定两个core
      //.master("local[*]") // 本地多线程，指定所有可用内核
      //.master("spark://master:7077") // 连接到指定的 Spark standalone cluster master，需要指定端口。
      //.master("mesos://master:port") // 连接知道指定的mesos集群，需要指定端口
      // yarn-client 客户端模式，连接到yarn集群。 需要配置 HADDOP_CONF_DIR
      // yarn-cluster 集群模式，连接到yarn集群。 需要配置 HADDOP_CONF_DIR
      .getOrCreate()
    /**
      * 2. 加载数据
      */
    val lines = spark.sparkContext.textFile("/Applications/fluency/data/spark/student.txt")
    //val lines = spark.sparkContext.textFile("hdfs://localhost:9000/spark/parquet/cubeplus1") // 读取hdfs中的数据
    // 创建rdd的另外一种方式
    val zoo = spark.sparkContext.parallelize(List("monkey", "panda", "donkey", "bear", "lion", "bear"))
    zoo.collect().foreach(println)
    val animals = zoo.flatMap(_.split(" "))
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
