package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadJsonFileInfo {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("ReadFileInfo")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val df:DataFrame = spark.read.text(this.getClass.getClassLoader.getResource("person.json").getPath)
    df.printSchema
    df.show()

    println("-----")
    println(df.count())
  }
}
