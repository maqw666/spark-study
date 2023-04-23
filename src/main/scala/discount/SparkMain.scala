package discount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMain {
  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val dataRDD = sc.textFile(this.getClass.getClassLoader.getResource("sales.txt").getPath)
    val salesRecordRDD:RDD[SalesRecord] = dataRDD.map(row =>{
      val colValues = row.split(",")
      SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })
    //总金额：Spark RDD API:892.0
    println("Spark RDD API:"+salesRecordRDD.map(_.itemValue).sum)
    //需求1
   /* val moneyRDD:RDD[Double] = salesRecordRDD
    println("customer RDD API:"+moneyRDD.collect().toBuffer)
    println("customer RDD API:" + moneyRDD.sum())
    //需求2 给rdd增加 action算子，求所有itemValue总和
    val totalResult:Double = salesRecordRDD.getTotalValue
    println("total_result"+totalResult)
    //自定义RDD,将RDD转换成为新的RDD
    val resultCountRDD:CustomerRDD = salesRecordRDD.discount(0.8)*/
    //println(resultCountRDD.collect().toBuffer)
    sc.stop()
  }
}

case class SalesRecord(val transactionId:String,
                       val customerId:String,
                       val itemId:String,
                       val itemValue:Double) extends Serializable
