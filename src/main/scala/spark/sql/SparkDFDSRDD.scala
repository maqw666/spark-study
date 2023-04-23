package spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import spark.sql.ReadTxtFileInfo2.Person

object SparkDFDSRDD {

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

    //1.rdd 转df
    val df1: DataFrame = personRDD.toDF
    df1.show
    //2.RDD转DS
    val ds1: Dataset[Person] = personRDD.toDS
    ds1.show
    //3.DF转RDD
    val rdd2: RDD[Row] = df1.rdd
    println(rdd2.collect.toList)
    //4.DS转RDD
    val rdd3:RDD[Person] = ds1.rdd
    println(rdd3.collect.toList)
    //5.DS转DF
    val df2: DataFrame = ds1.toDF
    df2.show
    //6.DF转DS
    val ds3: Dataset[Person] = df2.as[Person]
    ds3.show
    spark.close()
    sc.stop()



    spark.stop()
  }
}


