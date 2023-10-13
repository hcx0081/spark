package com.spark.operation.transformation.single.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapParOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapParOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    // 【1，2】【3，4，5】
    val mapRdd1: RDD[Int] = rdd.map(num => {
      println("----", num)
      num
    })
    
    // 【1，2】【3，4，5】
    val mapRdd2: RDD[Int] = rdd.map(num => {
      println("====", num)
      num
    })
    
    // (----,3)
    // (----,1)
    // (----,2)
    // (----,4)
    // (----,5)
    // (====,3)
    // (====,4)
    // (====,5)
    // (====,1)
    // (====,2)
    
    sc.stop()
  }
}
