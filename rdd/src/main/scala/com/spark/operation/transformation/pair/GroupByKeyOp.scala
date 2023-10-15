package com.spark.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupByKeyOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1)
    ), 2)
    
    val groupByKeyRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    
    groupByKeyRdd.foreach(println)
    // (a,CompactBuffer(1, 1))
    // (b,CompactBuffer(1, 1, 1))
    
    sc.stop()
  }
}
