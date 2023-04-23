package spark.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDataFromMysql {
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer").getOrCreate()
    //3.读取mysql表的数据
    //3.1 指定mysql连接地址
    val url = "jdbc:mysql://node03:3306/mydb?characterEncoding=UTF-8"
    //3.2 指定要加载的表名
    val tableName = "jobdetail"
    //3.3配置连接数据库的相关属性
    val props = new Properties()
    //用户名
    props.setProperty("user","root")
    //密码
    props.setProperty("password","123456")

    val mysqlDF:DataFrame = spark.read.jdbc(url,tableName,props)
    //打印shema信息
    mysqlDF.printSchema()
    //展示数据，默认展示top20
    mysqlDF.show()
    mysqlDF.createTempView("job_detail")
    spark.sql("select * from job_detail where city = '广东'").show()
    spark.stop()
  }

}
