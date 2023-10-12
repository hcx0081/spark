package com.spark.operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    
    // def mapFunc(num: Int): Int = num * 2
    // val mapRdd: RDD[Int] = rdd.map(mapFunc)
    
    // val mapRdd: RDD[Int] = rdd.map((num: Int) => {
    //   num * 2
    // })
    
    // val mapRdd: RDD[Int] = rdd.map((num: Int) => num * 2)
    
    // val mapRdd: RDD[Int] = rdd.map((num) => num * 2)
    
    // val mapRdd: RDD[Int] = rdd.map(num => num * 2)
    
    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    
    mapRdd.collect().foreach(println)
    
    sc.stop()
  }
}
