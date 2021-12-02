package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
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

        // Spark config

        SparkConf conf = new SparkConf().setAppName("samehost").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read file

        JavaRDD<String> july1 = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> aug1 = sc.textFile("in/nasa_19950801.tsv");

        // Only get the host

        JavaRDD<String> july1_host = july1.map(lines->lines.split("\t")[0]);
        JavaRDD<String> aug1_host =  aug1.map(lines->lines.split("\t")[0]);

        // Intersection

        JavaRDD<String> intersect = july1_host.intersection(aug1_host);

        // Remove headers
        JavaRDD<String> clean = intersect.filter(line -> isNotHeader(line));

        // Remove 'HOST'

        JavaRDD<String> finalclean = clean.filter(line -> !line.equals ("host"));

        // Save file as output
        finalclean.saveAsTextFile("out/nasa_logs_same_hosts.csv");

    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
