package com.spark.rdd.operation.transformation.doub

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ZipOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ZipOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(6, 7, 8, 9, 10), 2)
    
    val zipRdd: RDD[(Int, Int)] = rdd1.zip(rdd2)
    
    zipRdd.collect().foreach(println)
    // (1,6)
    // (2,7)
    // (3,8)
    // (4,9)
    // (5,10)
    
    sc.stop()
  }
}
