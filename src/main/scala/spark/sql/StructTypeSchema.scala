package spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StructTypeSchema {

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
    val rdd1:RDD[Array[String]] = sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath())
      .map(x=>x.split(" "))
    //把rdd与样例类进行关联
    val personRDD:RDD[Row] = rdd1.map(x=>Row(x(0),x(1),x(2).toInt))
    //把rdd转换成DataFrame
    val shema:StructType = StructType (Seq(
      StructField("id", StringType,true),
        StructField("name", StringType,true),
        StructField("age", IntegerType,true)
    ))

    val df:DataFrame = spark.createDataFrame(personRDD,shema)
    df.printSchema()
    df.show()

    //用sql 方式查询结构化数据
    df.createTempView("person")
    spark.sql("select * from person").show()
    spark.stop()
  }
}


