package com.learn.sparkpairRdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  If it’s a new element, combineByKey() uses a function we provide, called createCombiner(),
  to create the initial value for the accumulator on that key.
  It’s important to note that this happens the first time a key is found in each partition,
  rather than only the first time the key is found in the RDD.

  If it is a value we have seen before while processing that partition,
  it will instead use the provided function, mergeValue(),
  with the current value for the accumulator for that key and the new value.

  Since each partition is processed independently,
  we can have multiple accumulators for the same key.
  When we are merging the results from each partition,
  if two or more partitions have an accumulator for the same key
  we merge the accumulators using the user-supplied mergeCombiners() function.
 */
object CombineByKeyExample {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRdd = cleanedLines.map(line => (line.split(",")(3), line.split(",")(2).toDouble))

    val createCombiner = (x: Double) => (1, x)
    val mergeValue = (avgCount: AvgCount, x: Double) => (avgCount._1 + 1, avgCount._2 + x)
    val mergeCombiners = (avgCountA: AvgCount, avgCountB: AvgCount) => (avgCountA._1 + avgCountB._1, avgCountA._2 + avgCountB._2)

    val housePriceTotal = housePricePairRdd.combineByKey(createCombiner, mergeValue, mergeCombiners)

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount._2 / avgCount._1)
    for ((bedrooms, avgPrice) <- housePriceAvg.collect()) println(bedrooms + " : " + avgPrice)
  }

  type AvgCount = (Int, Double)
}
