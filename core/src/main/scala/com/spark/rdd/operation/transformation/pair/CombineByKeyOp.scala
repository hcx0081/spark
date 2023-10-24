package com.spark.rdd.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CombineByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
      ), 2)
    
    val combineByKeyRdd: RDD[(String, Int)] = rdd.combineByKey(
      v => {
        v * 100
      },
      (x, y) => {
        Math.max(x, y)
      },
      (x, y) => {
        x + y
      }
      )
    
    combineByKeyRdd.collect().foreach(println)
    // (b,100)
    // (a,100)
    
    sc.stop()
  }
}
