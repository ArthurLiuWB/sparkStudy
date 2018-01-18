package cn.wenbo.streaming

import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._


object SocketWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Streaming Words Count")
    // scala 中，创建的是StreamingContext
    val ssc = new StreamingContext(sc, Seconds(10))
    // 基于socket数据源的实时计算
    val lines = ssc.socketTextStream("localhost", 9999)
    //val words = lines.map(_.split(" "))
     // .map(word => (word, 1))
     // .reduceByKey(_+_)
    Thread.sleep(5000)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
