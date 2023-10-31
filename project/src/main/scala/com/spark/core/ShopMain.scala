package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ShopMain {
  // 需求编号
  var number = 1
  
  def main(args: Array[String]): Unit = {
    if (number == 1) {
      number1Method2()
    }
    if (number == 2) {
    }
  }
  
  def number1Method2(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShopMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID,产品ID,下单品类ID,产品ID,支付品类ID,产品ID,城市ID
    val actionRdd: RDD[String] = sc.textFile("data-core/user_visit_action.txt")
    actionRdd.cache()
    
    /*
    * 统计top10热门品类（点击数量 > 下单数量 > 支付数量）
    *  */
    // 统计品类的点击数量
    val flatRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(action => {
      val data: Array[String] = action.split("_")
      if (data(6) != "-1") {
        List((data(6), (1, 0, 0)))
      } else if (data(8) != "null") {
        val cids = data(8)
        val cidArr: Array[String] = cids.split(",")
        cidArr.map(id => (id, (0, 1, 0)))
      } else if (data(10) != "null") {
        val cids = data(10)
        val cidArr: Array[String] = cids.split(",")
        cidArr.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })
    val alysRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
    val result: Array[(String, (Int, Int, Int))] = alysRdd.sortBy(_._2, false).take(10)
    result.foreach(println)
    
    sc.stop()
  }
  
  def number1Method1(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShopMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID,产品ID,下单品类ID,产品ID,支付品类ID,产品ID,城市ID
    val actionRdd: RDD[String] = sc.textFile("data-core/user_visit_action.txt")
    actionRdd.cache()
    
    /*
    * 统计top10热门品类（点击数量 > 下单数量 > 支付数量）
    *  */
    // 统计品类的点击数量
    val clickActionRdd: RDD[String] = actionRdd.filter(action => {
      val data: Array[String] = action.split("_")
      data(6) != "-1"
    })
    val clickCountRdd: RDD[(String, Int)] = clickActionRdd.map(action => {
      val data: Array[String] = action.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _)
    // clickCountRdd.collect().foreach(println)
    
    // 统计品类的下单数量
    val orderActionRdd: RDD[String] = actionRdd.filter(action => {
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
    val payActionRdd: RDD[String] = actionRdd.filter(action => {
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
    val method = 1
    var alysRdd: RDD[(String, (Int, Int, Int))] = null
    if (method == 1) {
      val fullRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRdd.cogroup(orderCountRdd, payCountRdd)
      alysRdd = fullRdd.mapValues {
        case (clickIter, orderIter, payIter) => {
          var clickCount = 0
          var iter1 = clickIter.iterator
          if (iter1.hasNext) {
            clickCount = iter1.next()
          }
          var orderCount = 0
          var iter2 = orderIter.iterator
          if (iter2.hasNext) {
            orderCount = iter2.next()
          }
          var payCount = 0
          var iter3 = payIter.iterator
          if (iter3.hasNext) {
            payCount = iter3.next()
          }
          (clickCount, orderCount, payCount)
        }
      }
      alysRdd.collect().foreach(println)
    }
    if (method == 2) {
      val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRdd.map {
        case (cid, count) => {
          (cid, (count, 0, 0))
        }
      }
      val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRdd.map {
        case (cid, count) => {
          (cid, (0, count, 0))
        }
      }
      val rdd3: RDD[(String, (Int, Int, Int))] = payCountRdd.map {
        case (cid, count) => {
          (cid, (0, 0, count))
        }
      }
      val fullRdd: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
      alysRdd = fullRdd.reduceByKey((t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      })
    }
    
    val result: Array[(String, (Int, Int, Int))] = alysRdd.sortBy(_._2, false).take(10)
    result.foreach(println)
    
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