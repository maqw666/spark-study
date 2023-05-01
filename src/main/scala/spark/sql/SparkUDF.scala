package spark.sql

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUDF {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")
    val session:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame:DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat","yyyy/MM/dd HH:DD:SS")
      .option("header","true")
      .option("multiLine",true)
      .load("F:\\csv\\house.csv")
    frame.createOrReplaceTempView("house_sale")

    session.udf.register("house_udf",new UDF1[String,String]{
      //正则
      private val pattern:Pattern = Pattern.compile("^[0-9]*$")

      override def call(year: String):String = {
        val matcher:Matcher = pattern.matcher(year)
        if(matcher.matches()){
          year
        }else{
          "1990"
        }
      }
    },DataTypes.StringType)

    session.sql("select house_udf(house_age) from house_sale").show()

    session.close()

  }

}
