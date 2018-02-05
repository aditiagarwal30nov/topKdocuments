package com.mycompany.app;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;

import java.util.*;

public final class SparkWordCount {

  public static void main(String[] args) throws Exception {

    //create Spark context with Spark configuration
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("wordcount").set("spark.files.overwrite","true"));

    //set the input file
    JavaPairRDD<String, String> textFiles = sc.wholeTextFiles("datafiles");

    //word count process
    JavaPairRDD<String, Integer> counts = textFiles
    .flatMap(s -> {
        List<String> inputListByFileName = Arrays.asList(s._2.split(" "));
        ArrayList<String> outputListByFileName = new ArrayList<String>();
        for (String str : inputListByFileName) {
            outputListByFileName.add(str.replaceAll("[^a-zA-Z0-9]", ""));
        }
        return outputListByFileName.iterator();
    })
    .mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);

    //set the output folder
    counts.saveAsTextFile("outfile");
    //stop spark
  }

}
