package com.spark.operation.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val aggregateRdd: Int = rdd.aggregate(100)((x, y) => math.max(x, y), (x, y) => x + y)
    
    println(aggregateRdd)
    // 300
    
    sc.stop()
  }
}
