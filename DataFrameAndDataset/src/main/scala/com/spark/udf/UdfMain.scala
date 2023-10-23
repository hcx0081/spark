package com.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object UdfMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UdfMain")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    
    val df: DataFrame = Seq(("zs", 20), ("ls", 20), ("ww", 20)).toDF("name", "age")
    df.createOrReplaceTempView("user")
    
    spark.udf.register("prefixName", (arg: String) => {
      "name: " + arg
    })
    
    spark.sql("select prefixName(name), age from user").show()
    // +----------------+---+
    // |prefixName(name)|age|
    // +----------------+---+
    // |        Name: zs| 20|
    // |        Name: ls| 20|
    // |        Name: ww| 20|
    // +----------------+---+
    
    spark.stop()
  }
}
