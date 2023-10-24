package com.spark.rdd.operation.transformation.single.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RepartitionOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RepartitionOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    rdd.saveAsTextFile("output1") // 2个分区文件
    
    // val repartitionRdd: RDD[Int] = rdd.coalesce(4, true)
    val repartitionRdd: RDD[Int] = rdd.repartition(4)
    repartitionRdd.saveAsTextFile("output2") // 4个分区文件
    
    sc.stop()
  }
}
