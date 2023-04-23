package discount

import org.apache.spark.rdd.RDD

class CustomFunctions(rdd:RDD[SalesRecord]) {

  def getItemValue:RDD[Double] = rdd.map(x=>x.itemValue)
  //求所有itemValue总和
  def getTotalValue = rdd.map(x=>x.itemValue).sum
  def discount(discountPercentage:Double) = new CustomerRDD(rdd,discountPercentage)
}

object CustomFunctions{
  implicit def addIteblogCustomFunctions(rdd:RDD[SalesRecord]) = new CustomFunctions(rdd)
}
