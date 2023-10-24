package com.spark.rdd.operation.transformation.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionByOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("PartitionByOp")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 1), ("b", 1), ("b", 1), ("b", 1)
      ), 2)
    
    rdd.saveAsTextFile("output1") // 2个分区文件
    
    val partitionByRdd: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(4))
    
    partitionByRdd.saveAsTextFile("output2") // 4个分区文件
    
    sc.stop()
  }
}
