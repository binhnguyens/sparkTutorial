package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Int;
import scala.util.parsing.combinator.testing.Str;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        // Configuration
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("primenumbers").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read file
        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");

        // How many files are there
        System.out.println("Number of words: " + lines.count());

        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        System.out.println("Total numbers: " + numbers.count());

        // Print first 20 values
        System.out.println (numbers.take(20));

        // Filter out the empty ones
        JavaRDD<String> filtered = numbers.filter(line -> !line.isEmpty());

        // Take top 100
//        System.out.println (filtered.count());
//        JavaRDD<String> top100 = filtered.take(100);

        // Turn string into int
        JavaRDD<Integer> intnums = filtered.map(num -> Integer.valueOf(num));

        int summmed = intnums.reduce((x,y) -> x+y);

        System.out.println("Summation of values is: " + summmed);








//        // List them together
//        JavaRDD<String> nums = sc.parallelize(numbers);
//        List<String> numbers = nums.collect();

        // Get top 100 values
//        JavaRDD<String> taken_values = numbers.take(20);






    }
}
