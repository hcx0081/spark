package com.spark.rdd.operation.transformation.doub

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubtractOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SubtractOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3), 2)
    
    val subtractRdd: RDD[Int] = rdd1.subtract(rdd2)
    
    subtractRdd.collect().foreach(println)
    // 4
    // 5
    
    sc.stop()
  }
}
