package com.spark.shared.create.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object CreateAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("CreateAccumulator")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    
    // var sum = 0
    // rdd.foreach(num => sum += num)
    // println(sum) // 0
    
    val sum: LongAccumulator = sc.longAccumulator
    rdd.foreach(num => sum.add(num))
    println(sum.value) // LongAccumulator(id: 0, name: None, value: 15)
    
    sc.stop()
  }
}
