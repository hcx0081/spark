package com.spark.operation.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ReduceOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val reduceRdd: Int = rdd.reduce(_ + _)
    
    println(reduceRdd) // 15
    
    sc.stop()
  }
}
