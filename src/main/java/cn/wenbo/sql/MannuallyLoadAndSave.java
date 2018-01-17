package cn.wenbo.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MannuallyLoadAndSave {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("mannually save and load")
                .master("local")
                .getOrCreate();
        Dataset<Row> dataFrame1 = spark.read().format("json")
                .load("hdfs://192.168.87.6:9000/spark/sparksql.json");
        dataFrame1.write().format("csv")
                .save("hdfs://192.168.87.6:9000/spark/sparksql.csv");

    }
}
