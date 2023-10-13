package com.spark.operation.transformation.doub

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UnionOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UnionOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3), 2)
    
    val unionRdd: RDD[Int] = rdd1.union(rdd2)
    
    unionRdd.collect().foreach(println)
    // 1
    // 2
    // 3
    // 4
    // 5
    // 1
    // 2
    // 3
    
    sc.stop()
  }
}
