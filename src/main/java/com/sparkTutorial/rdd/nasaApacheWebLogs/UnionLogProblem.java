package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.util.parsing.combinator.testing.Str;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        //        Configure the work
        SparkConf conf = new SparkConf().setAppName("UnionProblem").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read the file
        JavaRDD<String> jul1 = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> aug1 = sc.textFile("in/nasa_19950801.tsv");

        // Union
        JavaRDD<String> unioned_logs = jul1.union(aug1);

        // Removing the headers
        JavaRDD<String> clean_logs = unioned_logs.filter(line->isNotHeader(line));

        JavaRDD<String> clean = clean_logs.sample(true, 0.1);

        // Save the clean log as a file
        clean.saveAsTextFile("out/UnionLogProblem");


    }
    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
