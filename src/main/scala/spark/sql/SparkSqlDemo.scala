package spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.sql.ReadTxtFileInfo2.Person

object SparkSqlDemo {

  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("SparkSqlDemo")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    //重要：添加隐式转换
    val sc:SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    val rdd1:RDD[Array[String]] = sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath())
      .map(x=>x.split(" "))
    //把rdd与样例类进行关联
    val personRDD:RDD[Person] = rdd1.map(x=>Person(x(0),x(1),x(2).toInt))

    //把rdd转换成DataFrame
    import spark.implicits._

    val personDF = personRDD.toDF
    //用sql 方式查询结构化数据
    personDF.createTempView("person")
    spark.sql("select * from person").show()
    spark.sql("select name from person").show()
    spark.sql("select age from person").show()
    spark.stop()
  }
}


