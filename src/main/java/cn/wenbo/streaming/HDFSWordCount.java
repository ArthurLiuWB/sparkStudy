package cn.wenbo.streaming;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.*;

public class HDFSWordCount {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf()
                .setAppName("spark streaming base on hdfs")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        // 首先使用javaStreamingConxtext的DStream方法读取hdfs目录下面的数据
        JavaDStream<String> lines = jsc.textFileStream("hdfs://192.168.87.6:9000/spark/");


        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
