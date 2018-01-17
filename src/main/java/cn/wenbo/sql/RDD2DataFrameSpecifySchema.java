package cn.wenbo.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

public class RDD2DataFrameSpecifySchema {
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("JavaSQL")
                .master("local")
                .getOrCreate();
        // 2. 创建RowRDD
        JavaRDD<Row> rowRDD = spark.sparkContext()
                // 创建普通RDD
                .textFile("file:\\F:\\test\\data\\student.txt", 1)
                // 转换成JavaRDD
                .toJavaRDD()
                // 转换成元素为Row类型的RDD
                .map(new Function<String, Row>() {
                    public Row call(String line) throws Exception {
                        String[] lineSplited = line.split(",");
                        return RowFactory.create(
                                lineSplited[0],
                                Integer.valueOf(lineSplited[1]),
                                lineSplited[2]);
                    }
                });
        // 3. 构造schema
        List<StructField> structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(structFields);
        // 4. 构造DataFrame
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        // 5. g创建临时试图
        df.createOrReplaceTempView("people");
        // 6. 查询视图
        Dataset<Row> results = spark.sql("select * from people");
        // 7.
        List<Row> rows = results.javaRDD().collect();
        //
        for (Row row: rows) {
            System.out.println(row);
        }

    }
}
