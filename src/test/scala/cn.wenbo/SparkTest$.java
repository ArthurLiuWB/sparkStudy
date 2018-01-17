object SparkTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkTest")
        .master("local")
      .getOrCreate()
    val file = spark.sparkContext.textFile("F:\\test\\data\\sparktest1.txt")
    System.out.println(file.collect().length)
    val rdd = file.flatMap( line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    rdd.collect()
    rdd.foreach(println)
  }
}
