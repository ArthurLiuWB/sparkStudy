package scala.wenbo.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用反射推断schema
  *
  * 注意：
  * 使用case class 开发spark程序
  * 使用基于放射的方式实现rdd到DataFrame的转换，就必须用object extends App的方式实现
  * 不能使用def main的方式
  *
  */
object RDD2DataFrameInfferSchema extends App {
 // def main(args: Array[String]): Unit = {
  // 创建配置文件
  val conf = new SparkConf()
    .setAppName("spark sql")
    .setMaster("local")
  // 创建sparkContext
  val sc = new SparkContext(conf)
  // 创建sqlContext
  val sqlContext = new SQLContext(sc)
  // 创建case class
  case class Student(name: String, age: Int, sex: String)
  // 普通的元素为case class的rdd
  val students = sc.textFile("file:\\F:\\test\\data\\student.txt")
    .map(line => line.split(","))
    .map(arr => Student(arr(0), arr(1).trim.toInt, arr(2)))
  // 将rdd隐式转成df，需要
  import sqlContext.implicits._
  // 直接使用rdd的toDF方法，将rdd转换成DataFrame
  val studentsDF = students.toDF()
  // 打印所有学生
  studentsDF.show()
  studentsDF.registerTempTable("student")
  val teenagerDF = sqlContext.sql("select * from student where age <= 18")
  // 打印青年学生
  teenagerDF.show()
  val teenagerRDD = teenagerDF.rdd
    .map(row => Student(row(0).toString, row(1).toString.toInt, row(2).toString))
    .collect()
    .foreach(stu => println(stu.name +":" + stu.sex+":"+stu.age))
  val teenagerRDD1 = teenagerDF.rdd
    .map(row => Student(row.getAs[String]("name"), row.getAs[Int]("age"), row.getAs[String]("sex")))
    .collect()
    .foreach(stu => println(stu.name +":" + stu.sex+":"+stu.age))

 //}

}
