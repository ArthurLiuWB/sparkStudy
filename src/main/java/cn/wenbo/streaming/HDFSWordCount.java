package cn.wenbo.streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.*;
import scala.Tuple2;

import java.util.Arrays;

public class HDFSWordCount {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf()
                .setAppName("spark streaming base on hdfs")
                .setMaster("local[2]");
       SparkSession spark = SparkSession.builder().getOrCreate();
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        // 首先使用javaStreamingConxtext的DStream方法读取hdfs目录下面的数据
        JavaRDD<String> lines = spark.read().textFile("hdfs://192.168.87.6:9000/spark/").toJavaRDD();

        JavaPairRDD<String,Integer> word =lines.flatMap(
                x -> (Arrays.asList(x.split(",")).iterator())
        ).mapToPair(
                x -> (new Tuple2<>(x,1)
                )
        ).reduceByKey((x,y) -> (x+y),10);
        // shuffle操作发生，提高reduceByKey的并行操作

        //spark.sparkContext().textFileStream("hdfs://192.168.87.6:9000/spark/");

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
