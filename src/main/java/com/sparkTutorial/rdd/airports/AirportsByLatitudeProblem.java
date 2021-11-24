package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.util.parsing.combinator.testing.Str;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

//        Config

        SparkConf conf = new SparkConf().setAppName("airports_latitude").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        Read file
        JavaRDD<String> airports = sc.textFile("in/airports.text");

//        Filter it out
        JavaRDD<String> airports_40 = airports.filter(line -> Float.valueOf (line.split(Utils.COMMA_DELIMITER)[6]) > 40);

//        Map
        JavaRDD<String> airports_by_latitude = airports_40.map(line ->
                {
                    String[] splits = line.split (Utils.COMMA_DELIMITER);
                    return StringUtils.join (new String []{splits[1],splits[6]}, ",");
                }
                );
        airports_by_latitude.saveAsTextFile("out/airports_by_latitude");
    }
}
