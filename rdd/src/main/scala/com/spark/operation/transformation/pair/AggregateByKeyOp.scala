package com.spark.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("b", 2), ("b", 3)
    ), 2)
    
    // val aggregateByKeyRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(
    //   (x, y) => {
    //     Math.max(x, y)
    //   },
    //   (x, y) => {
    //     println(x, y)
    //     Math.max(x, y)
    //   }
    // )
    //
    // aggregateByKeyRdd.foreach(println)
    
    sc.stop()
  }
}
