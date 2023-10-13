package com.spark.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilterOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FilterOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val filterRdd: RDD[Int] = rdd.filter(_ % 2 == 0)
    
    filterRdd.collect().foreach(println)
    // 2
    // 4
    
    sc.stop()
  }
}
