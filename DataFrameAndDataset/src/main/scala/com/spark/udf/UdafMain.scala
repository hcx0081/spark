package com.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

object UdafMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UdafMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    df.createOrReplaceTempView("user")
    
    spark.udf.register("myAvgUdaf", functions.udaf(new MyAvgUdaf()))
    
    spark.sql("select myAvgUdaf(age) from user").show()
    // +--------------+
    // |myavgudaf(age)|
    // +--------------+
    // |            20|
    // +--------------+
    
    spark.stop()
  }
}

class MyAvgUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = ???
  
  override def bufferSchema: StructType = ???
  
  override def dataType: DataType = ???
  
  override def deterministic: Boolean = ???
  
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???
  
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???
  
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???
  
  override def evaluate(buffer: Row): Any = ???
}


// case class Buff(var total: Long, var count: Long)
//
// // 强类型的用户自定义聚合函数
// class MyAvgUdaf extends Aggregator[Long, Buff, Long] {
//   // 初始值
//   override def zero: Buff = Buff(0, 0)
//
//   override def reduce(buff: Buff, in: Long): Buff = {
//     buff.total += in
//     buff.count += 1
//     buff
//   }
//
//   override def merge(b1: Buff, b2: Buff): Buff = {
//     b1.total = b1.total + b2.total
//     b1.count = b1.count + b2.count
//     b1
//   }
//
//   override def finish(buff: Buff): Long = buff.total / buff.count
//
//   override def bufferEncoder: Encoder[Buff] = Encoders.product
//
//   override def outputEncoder: Encoder[Long] = Encoders.scalaLong
// }