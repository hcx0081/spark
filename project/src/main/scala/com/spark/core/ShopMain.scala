package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ShopMain {
  // 需求编号
  var number = 1
  
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShopMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID,产品ID,下单品类ID,产品ID,支付品类ID,产品ID,城市ID
    val linesRdd: RDD[String] = sc.textFile("data-core/user_visit_action.txt")
    
    /*
    * 统计top10热门品类（点击数量 > 下单数量 > 支付数量）
    *  */
    if (number == 1) {
      // 统计品类的点击数量
      val clickActionRdd: RDD[String] = linesRdd.filter(action => {
        val data: Array[String] = action.split("_")
        data(6) != "-1"
      })
      val clickCountRdd: RDD[(String, Int)] = clickActionRdd.map(action => {
        val data: Array[String] = action.split("_")
        (data(6), 1)
      }).reduceByKey(_ + _)
      // clickCountRdd.collect().foreach(println)
      
      // 统计品类的下单数量
      val orderActionRdd: RDD[String] = linesRdd.filter(action => {
        val data: Array[String] = action.split("_")
        data(8) != "null"
      })
      val orderCountRdd: RDD[(String, Int)] = orderActionRdd.flatMap(action => {
        val data: Array[String] = action.split("_")
        val cids = data(8)
        val cidArr: Array[String] = cids.split(",")
        cidArr.map(id => (id, 1))
      }).reduceByKey(_ + _)
      // orderCountRdd.collect().foreach(println)
      
      // 统计品类的支付数量
      val payActionRdd: RDD[String] = linesRdd.filter(action => {
        val data: Array[String] = action.split("_")
        data(10) != "null"
      })
      val payCountRdd: RDD[(String, Int)] = payActionRdd.flatMap(action => {
        val data: Array[String] = action.split("_")
        val cids = data(10)
        val cidArr: Array[String] = cids.split(",")
        cidArr.map(id => (id, 1))
      }).reduceByKey(_ + _)
      // payCountRdd.collect().foreach(println)
      
      // 排序（利用元组的比较规则），并取前10个
      clickCountRdd.cogroup(orderCountRdd).mapValues(x => {
        (x._1.toList.head, x._2.toList.head)
      }).collect().foreach(println)
      
      
      // val fullRdd: RDD[(String, (Iterable[(Iterable[Int], Iterable[Int])], Iterable[Int]))] = clickCountRdd.cogroup(orderCountRdd).cogroup(payCountRdd)
      // fullRdd.collect().foreach(println)
    }
    
    if (number == 2) {
      
    }
    
    sc.stop()
  }
}

// 用户访问动作表
case class UserVisitAction(
                            date: String, // 用户点击行为的日期
                            user_id: Long, // 用户ID
                            session_id: String, // SessionID
                            page_id: Long, // 页面ID
                            action_time: String, // 动作时间点
                            search_keyword: String, // 用户搜索的关键词
                            click_category_id: Long, // 品类ID
                            click_product_id: Long, // 商品ID
                            order_category_ids: String, // 一次订单中所有品类的ID集合
                            order_product_ids: String, // 一次订单中所有商品的ID集合
                            pay_category_ids: String, // 一次支付中所有品类的ID集合
                            pay_product_ids: String, // 一次支付中所有商品的ID集合
                            city_id: Long // 城市ID
                          )