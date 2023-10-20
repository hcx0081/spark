package com.spark.create

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object CreateAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("CreateAccumulator")
    val sc = new SparkContext(sparkConf)
    
    val accumulator: LongAccumulator = sc.longAccumulator
    println(accumulator.value)
    
    sc.stop()
  }
}
