package com.learn.sparkbasics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
*/

object UnionExample {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[3]").setAppName("UnionExample")
    val sc = new SparkContext(conf)

    val julyrdd = sc.textFile("in/nasa_19950701.tsv")
    val augrdd = sc.textFile("in/nasa_19950801.tsv")

    val unionrdd = julyrdd.union(augrdd)

    val filteredunionrdd1 = unionrdd.filter(line => !(line.startsWith("host") && line.contains("bytes")))
    //val filteredunionrdd = unionrdd.filter(line => isNotHeader(line))

    val sample = filteredunionrdd1.sample(withReplacement = true, fraction = 0.1)

    sample.saveAsTextFile("output/unionExample/sample_nasa_logs.tsv")
  }

  //As an example, we can also create a udf and call it
  //def isNotHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))
}
