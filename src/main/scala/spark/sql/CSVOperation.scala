package spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CSVOperation {
  def main(args: Array[String]): Unit = {
     val sparkCSV: SparkConf = new SparkConf().setMaster("local[4]").setAppName("sparkCSV")
    val session: SparkSession = SparkSession.builder().config(sparkCSV).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session.read
      .format("csv")
     .option("timestampFormat", "yyyy-MM-dd")
      .option("header", "true")
      .load("F:\\csv\\")

    //asfsfas
    frame.createOrReplaceTempView("job_detail")
    frame.show

    val ret = session.sql("select id,name,age from job_detail")
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")

    //要出入数据的数据库要使用utf-8编码
    ret.write.mode(SaveMode.Append).jdbc("dfa","job",prop)

    session.close()



  }

}
