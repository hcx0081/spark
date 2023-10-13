package com.spark.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMapOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4, 5)), 2)
    
    val mapRdd: RDD[List[Int]] = rdd.map(list => list)
    // List(1, 2)
    // List(3, 4, 5)
    
    // val mapRdd: RDD[Int] = rdd.flatMap(list => list)
    // 1
    // 2
    // 3
    // 4
    // 5
    
    mapRdd.collect().foreach(println)
    
    sc.stop()
  }
}
