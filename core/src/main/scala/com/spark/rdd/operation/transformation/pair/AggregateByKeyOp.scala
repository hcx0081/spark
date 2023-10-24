package com.spark.rdd.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
      ), 2)
    
    val aggregateByKeyRdd: RDD[(String, Int)] = rdd.aggregateByKey(100)(
      (x, y) => { // x始终是100
        Math.max(x, y)
      },
      (x, y) => {
        x + y
      }
      )
    
    aggregateByKeyRdd.collect().foreach(println)
    // (b,100)
    // (a,100)
    
    sc.stop()
  }
}
