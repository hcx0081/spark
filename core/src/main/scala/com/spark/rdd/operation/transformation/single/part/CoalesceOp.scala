package com.spark.rdd.operation.transformation.single.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CoalesceOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    rdd.saveAsTextFile("output1") // 2个分区文件
    
    val coalesceRdd: RDD[Int] = rdd.coalesce(1)
    coalesceRdd.saveAsTextFile("output2") // 1个分区文件
    
    sc.stop()
  }
}
