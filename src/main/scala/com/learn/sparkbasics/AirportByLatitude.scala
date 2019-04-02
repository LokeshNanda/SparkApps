package com.learn.sparkbasics

/* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...

Illustrated best practices for using constants
*/

import com.learn.commons.Utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object AirportByLatitude {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[3]").setAppName("AirportByLatitude")
    val sc = new SparkContext(conf)

    val airportsRdd = sc.textFile("in/airports.text")

    val airportsFiltered = airportsRdd.filter(line => line.split(Utilities.COMMA_DELIMITER)(6).toFloat > 40)

    val nameAndAirport = airportsFiltered.map(line => {
      val splits = line.split(Utilities.COMMA_DELIMITER)
      splits(1) + "," + splits(6)
    })

    nameAndAirport.saveAsTextFile("output/airportbylatitude/airports_by_latitude.text")
  }
}
