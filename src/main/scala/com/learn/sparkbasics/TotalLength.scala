package com.learn.sparkbasics

//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.SparkContext._


object TotalLength {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]") //Means 2 threads are running in parallelss
    conf.setAppName("Initialization App")

    val sc = new SparkContext(conf)

    val distfile = sc.textFile("wordcount.txt")
    //val linelength = distfile.map(s => s.length)
    //linelength..persist(StorageLevel.MEMORY_ONLY)
    //val totallength = linelength.reduce((a,b) => a+b)
    //linelength.collect().foreach(println)

    val linelength = distfile.map(s => s.length).reduce((a,b) => a+b)
    println("Total length of lines: " + linelength)

  }
}

