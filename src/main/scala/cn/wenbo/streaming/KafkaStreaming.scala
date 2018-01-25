package cn.wenbo.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaStreaming {
  def main(args: Array[String]): Unit = {

    // initialize
    val conf = new SparkConf()
      .setAppName("kafka Streaming")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    // create topic
    val topic = Array("future")
    // create consumer group
    val group = "con-consumer-group"
    // consumer configuration
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      // if true, the offset will be commited auto
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](ssc,
      PreferConsistent, Subscribe[String, String](topic, kafkaParam))
    stream.map(s => (s.key(), s.value())).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
