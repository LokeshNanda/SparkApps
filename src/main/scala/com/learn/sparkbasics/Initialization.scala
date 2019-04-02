package com.learn.sparkbasics

// Code which will be common for all spark program

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Initialization {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Initialization App")

    val sc = new SparkContext(conf)
  }
}
