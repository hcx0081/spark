package com.spark.to

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object RddAndDataset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RddAndDataset")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ // 引入隐式转换规则
    
    /* RDD => Dataset */
    // val rdd: RDD[(String, Int)] = sc.makeRDD(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    // val ds: Dataset[(String, Int)] = rdd.toDS()
    // // val ds: Dataset[(String, Int)] = spark.createDataset(rdd)
    //
    // ds.show()
    
    /* Dataset => RDD */
    val ds: Dataset[(String, Int)] = spark.createDataset(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    val rdd: RDD[(String, Int)] = ds.rdd
    
    rdd.collect().foreach(println)
    
    spark.stop()
  }
}
