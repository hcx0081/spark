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
    
    val mapRdd: RDD[((String, String), Int)] = lines.map(data => {
      val strArr: Array[String] = data.split(" ")
      // ((省份, 广告), 1)
      ((strArr(1), strArr(4)), 1)
    })
    
    // ((省份, 广告), 1) => ((省份, 广告), 点击量)
    val reduceByKeyRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)
    
    /* 1 */
    
    // ((省份, 广告), 点击量) => (省份, (广告, 点击量))
    val chgRdd: RDD[(String, (String, Int))] = reduceByKeyRdd.map(data => (data._1._1, (data._1._2, data._2)))
    
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = chgRdd.groupByKey()
    
    val sortByRdd: RDD[(String, Seq[(String, Int)])] =
      groupRdd.map(data => {
        val top3: Seq[(String, Int)] = data._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        (data._1, top3)
      })
    
    /* 2 */
    
    // val sortByRdd: RDD[((String, String), Int)] = reduceByKeyRdd.sortBy(data => data._2, false)
    //
    // val groupRdd: RDD[(String, Iterable[((String, String), Int)])] = sortByRdd.groupBy(s => s._1._1)
    //
    // val result: RDD[(String, Iterable[String])] = groupRdd.map(data => {
    //   val top3: Iterable[String] = data._2.take(3).map(item => {
    //     item._1._2
    //   })
    //   (data._1, top3)
    // })
    
    sortByRdd.collect().foreach(println)
  }
}
