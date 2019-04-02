package com.learn.sparkbasics
import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val inputFile = "wordcount.txt"
    val outputFile = "output/wordcount/out"
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile)
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    //val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    val counts = words.map(word => (word, 1)).reduceByKey((x,y) => x+y)

    // Save the word count back out to a text file, causing evaluation.
    //This can be done in one more way
    /*
    val wordCounts = words.countByValue()
    for ((word,count) <- wordCounts){
      println(word + ":" + count)
    }
    * */
    counts.saveAsTextFile(outputFile)
  }
}
