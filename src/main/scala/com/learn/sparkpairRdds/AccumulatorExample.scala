package com.learn.sparkpairRdds

import com.learn.commons.Utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("AccumulatorApp").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val total = sparkContext.longAccumulator
    val missingSalaryMidPoint = sparkContext.longAccumulator
    val processedBytes = sparkContext.longAccumulator

    val responseRDD = sparkContext.textFile("in/2016-stack-overflow-survey-responses.csv")

    val responseFromCanada = responseRDD.filter(response => {

      processedBytes.add(response.getBytes().length)
      val splits = response.split(Utilities.COMMA_DELIMITER, -1)
      total.add(1)

      if (splits(14).isEmpty) {
        missingSalaryMidPoint.add(1)
      }
      splits(2) == "Canada"

    })

    println("Count of responses from Canada: " + responseFromCanada.count())
    println("Number of bytes processed: " + processedBytes.value)
    println("Total count of responses: " + total.value)
    println("Count of responses missing salary middle point: " + missingSalaryMidPoint.value)
  }
}
