package spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadTxtFileInfo2 {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("ReadFileInfo")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    //重要：添加隐式转换
    import spark.implicits._
    val sc:SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    val rdd1:RDD[Array[String]] = sc.textFile(this.getClass.getClassLoader.getResource("person.json").getPath())
      .map(x=>x.split(" "))
    //把rdd与样例类进行关联
    val personRDD:RDD[Person] = rdd1.map(x=>Person(x(0),x(1),x(2).toInt))
    //把rdd转换成DataFrame
    val df = personRDD.toDF
    df.printSchema()
    df.show()
    spark.stop()
  }
  case class Person(id:String,name:String,age:Int)
}


