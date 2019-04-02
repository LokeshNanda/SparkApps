package com.learn.sparkapptest

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApplication {
  def main(args:Array[String]): Unit ={

    //We need to create a sparkConfig and sparkContext
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("First Application")

    val sc = new SparkContext(conf)

    // Create an RDD
    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    rdd1.collect().foreach(println)
  }
}
