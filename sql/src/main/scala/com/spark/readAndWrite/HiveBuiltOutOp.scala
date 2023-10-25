package com.spark.readAndWrite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveBuiltOutOp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HiveBuiltOutOp")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    
    // val df: DataFrame = spark.createDataFrame(Seq(("zs", 20), ("ls", 20), ("ww", 20)))
    // df.createOrReplaceTempView("teacher")
    //
    // spark.sql("create table student(name string, age int)")
    //
    // spark.sql("show tables").show()
    
    System.getProperties.list(System.out)
    
    spark.stop()
  }
}
