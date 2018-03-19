package cn.wenbo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 案例：
  * 热点搜索词滑动统计：
  * 每隔十秒钟，统计最近六十秒的搜索词的搜索频次
  * 打印top20的搜索词及出现次数
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
      .setAppName(" spark Window process! ")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sc, Seconds(1))
    // tcp 接收数据生成dstream
    val searchLogsDstream = ssc.socketTextStream("localhost", 9999)
    // 过滤数据
    val searchWordsDstream = searchLogsDstream.map( _.split(" ")(1))
    // 将三次转换成pair
    val searchWordPairsDStream = searchWordsDstream.map( searchWord => (searchWord, 1))
    // 进行窗口操作统计
    val searchWordCountsDstream = searchWordPairsDStream.reduceByKeyAndWindow(
      (v1:Int, v2:Int) => v1 + v2, // 统计方法
      Seconds(60), // 滑动窗口时间
      Seconds(10) // 滑动间隔
    )
    val finalDstream = searchWordCountsDstream.transform(
      // dstream里面的rdd
      searchWordCountsRDD => {
        // rdd里面的tuple反转
        val countSearchWordsRDD = searchWordCountsRDD.map(tuple => (tuple._2, tuple._1))
        // 根据key值进行排序
        val sortedWordCountsRDD = countSearchWordsRDD.sortByKey(false)
        // 再次反转
        val sortedSearchWordCountRDD = sortedWordCountsRDD.map(tuple => (tuple._2, tuple._1))
        // 统计前20的数据
        val top20 = sortedSearchWordCountRDD.take(20)
        // 遍历打印前20的数据
        for (tuple <- top20) {
          print(tuple)
        }
        searchWordCountsRDD
      }
    )
    // action 为了触发job
    finalDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
