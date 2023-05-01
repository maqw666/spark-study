package spark.sql

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 多转一
 */
object SparkUDAF {
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

    session.udf.register("udaf",new MyAverage)

    session.sql("select floor,udaf(house_sale_money) as avg_money from house_sale group by floor").show()
    frame.printSchema()
    session.stop()
    session.close()
  }
}

class MyAverage extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {StructType(StructField("house_sale_money",DoubleType) :: Nil)}
  override def bufferSchema: StructType = {
    StructType(StructField("sum",DoubleType) :: StructField("count",LongType):: Nil)
  }
  //返回值的类型
  override def dataType: DataType = DoubleType
  //对于相同的输入是否一直返回相同输出
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //用于存储不同类型的楼房的总成交额
    buffer(0) = 0D
    //用于存储不同类型的楼房的总个数
    buffer(1) = 0L
  }
  //相同Execute 间的数据合并-分区间聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getDouble(0) + input.getDouble(0);
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) +buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
