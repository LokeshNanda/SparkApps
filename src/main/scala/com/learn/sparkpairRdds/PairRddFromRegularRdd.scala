package com.learn.sparkpairRdds

import org.apache.spark.{SparkConf, SparkContext}

/*
  https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
  https://data-flair.training/forums/topic/explain-coalesce-operation/
 */
object PairRddFromRegularRdd {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    val regularRDDs = sc.parallelize(inputStrings)

    //val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(1)))
    val pairRDD = regularRDDs.map(s => {
      val splits = s.split(" ")
      splits(0) + "," + splits(1)
    })
    pairRDD.coalesce(1).saveAsTextFile("output/pair_rdd_from_regular_rdd")
  }
}
