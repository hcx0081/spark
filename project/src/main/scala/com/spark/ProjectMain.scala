package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectMain {
  /**
   * 统计每一个省份的每一个广告的点击数量排行的Top3
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ProjectMain")
    val sc = new SparkContext(sparkConf)
    
    // 时间戳 省份 城市 用户 广告
    val lines: RDD[String] = sc.textFile("data/agent.log")
    
    
  }
}
