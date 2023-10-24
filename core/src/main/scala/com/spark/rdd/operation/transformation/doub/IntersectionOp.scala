package com.spark.rdd.operation.transformation.doub

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IntersectionOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("IntersectionOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3), 2)
    
    val intersectionRdd: RDD[Int] = rdd1.intersection(rdd2)
    
    intersectionRdd.collect().foreach(println)
    // 2
    // 1
    // 3
    
    sc.stop()
  }
}
