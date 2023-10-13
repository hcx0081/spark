package com.spark.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitionsWithIndexOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val mapRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 0) {
        iter
      } else {
        Nil.iterator
      }
    })
    
    mapRdd.collect().foreach(println)
    
    sc.stop()
  }
}
