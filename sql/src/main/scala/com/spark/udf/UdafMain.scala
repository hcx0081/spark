package com.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UdafMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UdafMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    df.createOrReplaceTempView("user")
    
    spark.udf.register("myAvgUdaf", new MyAvgUdaf())
    // spark.udf.register("myAvgUdaf", functions.udaf(new MyAvgUdaf()))
    
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
  override def inputSchema: StructType = {
    StructType(Array(
      StructField("age", IntegerType)
      ))
  }
  
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("total", LongType),
      StructField("count", LongType)
      ))
  }
  
  override def dataType: DataType = LongType
  
  override def deterministic: Boolean = true
  
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // buffer(0) = 0L
    // buffer(1) = 0L
    buffer.update(0, 0L)
    buffer.update(1, 0L)
  }
  
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getLong(0) + input.getInt(0))
    buffer.update(1, buffer.getLong(1) + 1)
  }
  
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
    buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
  }
  
  override def evaluate(buffer: Row): Any = buffer.getLong(0) / buffer.getLong(1)
}


// case class Buff(var total: Long, var count: Long)
//
// // 强类型的用户自定义聚合函数
// class MyAvgUdaf extends Aggregator[Long, Buff, Long] {
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