/* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
package com.learn.sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbers {
  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("SumOfNumbersApp")
    val sc = new SparkContext(conf)

    val rawdata = sc.textFile("in/prime_nums.text")

    val numbers = rawdata.flatMap(line => line.split("\\s++"))

    val validNumbers = numbers.filter(number => !number.isEmpty())

    val intNumbers = validNumbers.map(number => number.toInt)

    println("Sum is : " + intNumbers.reduce((a,b) => a+b))

  }
}
