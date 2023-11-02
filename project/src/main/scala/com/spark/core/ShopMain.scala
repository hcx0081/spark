package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object ShopMain {
  // 需求编号
  var number = 1
  
  def main(args: Array[String]): Unit = {
    /*
    * 统计top10热门品类（点击数量 > 下单数量 > 支付数量）
    *  */
    if (number == 1) {
      number1Method3()
    }
    if (number == 2) {
    }
  }
  
  // 没有shuffle，性能更好
  def number1Method3(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShopMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID,产品ID,下单品类ID,产品ID,支付品类ID,产品ID,城市ID
    val actionRdd: RDD[String] = sc.textFile("data-core/user_visit_action.txt")
    actionRdd.cache()
    
    val acc = new MyAccumulator
    sc.register(acc)
    
    actionRdd.foreach(action => {
      val data: Array[String] = action.split("_")
      if (data(6) != "-1") {
        acc.add((data(6), "click"))
      } else if (data(8) != "null") {
        val cids = data(8)
        val cidArr: Array[String] = cids.split(",")
        cidArr.foreach(id => acc.add(id, "order"))
      } else if (data(10) != "null") {
        val cids = data(10)
        val cidArr: Array[String] = cids.split(",")
        cidArr.foreach(id => acc.add(id, "pay"))
      }
    })
    
    val hotCategory: Iterable[HotCategory] = acc.value.values
    val sortedList: List[HotCategory] = hotCategory.toList.sortWith((left, right) => {
      if (left.clickCount > right.clickCount) {
        true
      } else if (left.clickCount == right.clickCount) {
        if (left.orderCount > right.orderCount) {
          true
        } else if (left.orderCount == right.orderCount) {
          left.payCount > right.payCount
        } else {
          false
        }
      } else {
        false
      }
    })
    sortedList.take(10).foreach(println)
    
    sc.stop()
  }
  
  def number1Method2(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ShopMain")
    val sc = new SparkContext(sparkConf)
    
    // 日期 用户ID SessionID 页面ID 动作时间 搜索关键字,点击品类ID,产品ID,下单品类ID,产品ID,支付品类ID,产品ID,城市ID
    val actionRdd: RDD[String] = sc.textFile("data-core/user_visit_action.txt")
    actionRdd.cache()
    
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

/*
* IN: (品类ID, 行为类型)
* OUT: (品类ID, (点击数量, 下单数量, 支付数量))
*  */
class MyAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
  private var hcMap = mutable.Map[String, HotCategory]()
  
  override def isZero: Boolean = hcMap.isEmpty
  
  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new MyAccumulator
  
  override def reset(): Unit = hcMap.clear()
  
  override def add(v: (String, String)): Unit = {
    val cid: String = v._1
    val actionType: String = v._2
    val hotCategory: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
    if (actionType == "click") {
      hotCategory.clickCount += 1
    }
    if (actionType == "order") {
      hotCategory.orderCount += 1
    }
    if (actionType == "pay") {
      hotCategory.payCount += 1
    }
    hcMap.update(cid, hotCategory)
  }
  
  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    var map1 = this.value
    var map2 = other.value
    
    map2.foreach {
      case (cid, hc) => {
        val hotCategory: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
        hotCategory.clickCount += hc.clickCount
        hotCategory.orderCount += hc.orderCount
        hotCategory.payCount += hc.payCount
        map1.update(cid, hotCategory)
      }
    }
  }
  
  override def value: mutable.Map[String, HotCategory] = hcMap
}

case class HotCategory(cid: String, var clickCount: Int, var orderCount: Int, var payCount: Int)

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