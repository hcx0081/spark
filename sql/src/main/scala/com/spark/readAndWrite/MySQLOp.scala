package com.spark.readAndWrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object MySQLOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MySQLOp")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "200081")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/spark?serverTimezone=UTC", "user", prop)
    df.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |  zs| 20|
    // |  ls| 20|
    // |  ww| 20|
    // +----+---+
    
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark?serverTimezone=UTC", "temp_user", prop)
    
    spark.stop()
  }
}
