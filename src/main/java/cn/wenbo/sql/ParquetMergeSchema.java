package cn.wenbo.sql;

import org.apache.spark.sql.SparkSession;

public class ParquetMergeSchema {
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("JavaSQL")
                .master("local")
                .getOrCreate();
        // 2. 创建df，并且写入hdfs

    }
}
