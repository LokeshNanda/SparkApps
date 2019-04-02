package com.learn.sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*     "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

object SameHostsProblem {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[3]").setAppName("SameHostApp")
    val sc = new SparkContext(conf)

    val rddfile1 = sc.textFile("in/nasa_19950701.tsv")
    val rddfile2 = sc.textFile("in/nasa_19950801.tsv")

    val file1 = rddfile1.map(x => x.split("\t")(0))
    val file2 = rddfile2.map(x => x.split("\t")(0))

    val res = file1.intersection(file2)

    val cleanedRes = res.filter(host => host != "host")
    cleanedRes.saveAsTextFile("output/samehostproblem/nasa_logs_same_hosts.csv")
  }
}
