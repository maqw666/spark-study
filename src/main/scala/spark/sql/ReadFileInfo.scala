package spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFileInfo {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("ReadFileInfo")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val df:DataFrame = spark.read.text(this.getClass.getClassLoader.getResource("person.txt").getPath)
    df.printSchema
    df.show()

    println("-----")
    println(df.count())
  }
}
