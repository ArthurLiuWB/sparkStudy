package cn.wenbo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 监控一个HDFS目录，每隔一段时间扫描一次目录出现的新文件。
  *
  */
object HDFSStreaming {
  val sc = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Streaming Words Count")
  // scala 中，创建的是StreamingContext
  val ssc = new StreamingContext(sc, Seconds(10))

  ssc.start()
  ssc.awaitTermination()
}
