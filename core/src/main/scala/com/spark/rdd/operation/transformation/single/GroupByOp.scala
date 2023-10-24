package com.spark.rdd.operation.transformation.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupByOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    
    val groupByRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    
    groupByRdd.collect().foreach(println)
    // (0,CompactBuffer(2, 4))
    // (1,CompactBuffer(1, 3, 5))
    
    sc.stop()
  }
}
