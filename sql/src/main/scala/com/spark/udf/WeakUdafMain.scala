package com.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object WeakUdafMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WeakUdafMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    df.createOrReplaceTempView("user")
    
    // 弱类型
    // spark.udf.register("myAvgUdaf", new MyAvgUdaf())
    
    spark.sql("select myAvgUdaf(age) from user").show()
    // +--------------+
    // |myavgudaf(age)|
    // +--------------+
    // |            20|
    // +--------------+
    
    spark.stop()
  }
}

// private class MyAvgUdaf extends UserDefinedAggregateFunction {
//   override def inputSchema: StructType = {
//     StructType(Array(
//       StructField("age", IntegerType)
//     ))
//   }
//
//   override def bufferSchema: StructType = {
//     StructType(Array(
//       StructField("total", LongType),
//       StructField("count", LongType)
//     ))
//   }
//
//   override def dataType: DataType = LongType
//
//   override def deterministic: Boolean = true
//
//   override def initialize(buffer: MutableAggregationBuffer): Unit = {
//     // buffer(0) = 0L
//     // buffer(1) = 0L
//     buffer.update(0, 0L)
//     buffer.update(1, 0L)
//   }
//
//   override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//     buffer.update(0, buffer.getLong(0) + input.getInt(0))
//     buffer.update(1, buffer.getLong(1) + 1)
//   }
//
//   override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//     buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
//     buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
//   }
//
//   override def evaluate(buffer: Row): Any = buffer.getLong(0) / buffer.getLong(1)
// }