package cn.wenbo.streaming

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._


object SocketStreaming {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Streaming Words Count")
    // scala 中，创建的是StreamingContext
    val ssc = new StreamingContext(sc, Seconds(10))
    /**
     * spark streaming base on socket source
     */
    val lines = ssc.socketTextStream("localhost", 9999)

    /**
     * word count
     */
//    val arr = lines.map(line => line.split(" "))
//    val words = arr.flatMap(x => x)
//    val wordPair = words.map( x => (x, 1))
//    val wordCount = wordPair.reduceByKey((x, y) => x + y)
    //wordCount.print()
    /**
      * write as txt file on local filesystem
      */
    //wordCount.saveAsTextFiles("/Applications/fluency/data/spark", "txt")

    /**
      * write as squence file on hdfs filesystem
      */
    //wordCount.saveAsHadoopFiles[SequenceFileOutputFormat[Text, LongWritable]](
    //  "hdfs://localhost:9000/spark/parquet","txt"
    //)
    //wordCount.saveAsTextFiles("hdfs://localhost:9000/spark/parquet","txt")
    /**
     * filter line contains "error" word
     */
    //lines.filter(_.contains("error")).print()
    /**
      * foreachRdd save item to filesystem
      */
    val structType = StructType(
      Array(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("sex", StringType, nullable = true)
      )
    )
    val arr = lines.map(line => line.split(" "))
    val spark = SparkSession.builder().getOrCreate()
    arr.foreachRDD(rdd => {
        val rowRdd = rdd.map(arr =>
          Row(arr(0), arr(1).trim.toInt, arr(2)))
        val dataFrame = spark.createDataFrame(rowRdd, structType)
        dataFrame.show()
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
