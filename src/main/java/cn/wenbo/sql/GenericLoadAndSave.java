package cn.wenbo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenericLoadAndSave {
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("JavaSQL")
                .master("local")
                .getOrCreate();
        Dataset<Row> userDF = spark.read().load("hdfs://192.168.87.6:9000/spark/parquet/cube");
        userDF.show();
        userDF.select("value").write().save("hdfs://192.168.87.6:9000/spark/parquet/cube2");
        Dataset<Row> userDF1 = spark.read().load("hdfs://192.168.87.6:9000/spark/parquet/cube2");
        userDF1.show();
    }
}
