package com.learn.sparkpairRdds
import com.learn.commons.Utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CollectAsMapExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")

    val countryAndAirportNameAndPair = lines.map(airport => (airport.split(Utilities.COMMA_DELIMITER)(3),
      airport.split(Utilities.COMMA_DELIMITER)(1)))

    val airportsByCountry = countryAndAirportNameAndPair.groupByKey()

    for ((country, airportName) <- airportsByCountry.collectAsMap()) println(country + ": " + airportName.toList)
  }
}
