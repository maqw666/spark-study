package discount

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class CustomerRDD(prev:RDD[SalesRecord], discountPercentage:Double) extends RDD[SalesRecord](prev){
  override def compute(split:Partition,context:TaskContext):Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split,context).map(salesRecord =>{
      //打折后的金额
      val discount = salesRecord.itemValue * discountPercentage
      SalesRecord(salesRecord.transactionId,salesRecord.customerId,salesRecord.itemId,discount)
    })
  }
  //继承getPartitions方法
  override protected def getPartitions:Array[Partition] =
    firstParent[SalesRecord].partitions

}
