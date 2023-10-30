package com.spark.adv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectMain {
  /**
   * 统计每一个省份的每一个广告的点击数量排行的Top3
   */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ProjectMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID和产品ID,下单品类ID和产品ID,支付品类ID和产品ID,城市ID
    val lines: RDD[String] = sc.textFile("data/user_visit_action.csv")
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