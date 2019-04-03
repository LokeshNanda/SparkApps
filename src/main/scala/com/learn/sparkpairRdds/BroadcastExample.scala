package com.learn.sparkpairRdds

import com.learn.commons.Utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object BroadcastExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val postCodeMap = sparkContext.broadcast(loadPostCodeMap())
    //print(postCodeMap.value)
    //FY7 -> Wyre, SG8 -> North Hertfordshire, WV10 -> Wolverhampton, IG1

    val makerSpaceRdd = sparkContext.textFile("in/uk-makerspaces-identifiable-data.csv")

    // isDefined == NonEmpty
    val regions = makerSpaceRdd
      .filter(line => line.split(Utilities.COMMA_DELIMITER, -1)(0) != "Timestamp")
      .filter(line => getPostPrefix(line).isDefined)
      .map(line => postCodeMap.value.getOrElse(getPostPrefix(line).get, "Unknown"))

    for ((region, count) <- regions.countByValue()) println(region + " : " + count)
  }

  //Option[T] can be either Some[T] or None object
  //https://www.tutorialspoint.com/scala/scala_options.htm

  def getPostPrefix(line: String): Option[String] = {
    val splits = line.split(Utilities.COMMA_DELIMITER, -1)
    val postcode = splits(4)
    if (postcode.isEmpty) None else Some(postcode.split(" ")(0))
  }

  def loadPostCodeMap(): Map[String, String] = {
    Source.fromFile("in/uk-postcode.csv").getLines.map(line => {
      val splits = line.split(Utilities.COMMA_DELIMITER, -1)
      splits(0) -> splits(7)
    }).toMap
  }
}
