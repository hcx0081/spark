package com.spark.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionerMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("NoPersistMain")
    val sc = new SparkContext(sparkConf)
    
    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxx"),
      ("cba", "xxx"),
      ("wnba", "xxx"),
      ("wcba", "xxx"),
      ("nba", "xxx")
    ), 2)
    
    val partitionByRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    
    partitionByRdd.saveAsTextFile("output") // 4个分区文件
    
    sc.stop()
  }
  
  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 4
    
    override def getPartition(key: Any): Int = {
      if (key == "nba") 0 else 1
    }
  }
}
