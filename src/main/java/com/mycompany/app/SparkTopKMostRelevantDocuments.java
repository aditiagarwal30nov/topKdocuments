package com.mycompany.app;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public final class SparkTopKMostRelevantDocuments {

  public static void main(String[] args) throws Exception {

    //create Spark context with Spark configuration
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("TopKMostRelevantDocuments").set("spark.files.overwrite","true"));

    //set the input files
    JavaPairRDD<String, String> textFiles = sc.wholeTextFiles("datafiles");
    List<String> stopWords = sc.textFile("stopwords.txt").collect();

    //stage 1 starting
    JavaPairRDD<String, Integer> stage1output = textFiles
    .flatMap(s -> {
        List<String> inputListByFileName = Arrays.asList(s._2.split(" "));
        ArrayList<String> outputListByFileName = new ArrayList<String>();
        Path p = Paths.get(s._1);
        String filename = p.getFileName().toString();
        for (String str : inputListByFileName) {
            str = str.replaceAll("[^a-zA-Z0-9']", "").trim().toLowerCase();
            if(!stopWords.contains(str))
            outputListByFileName.add(str + "@" +  (filename.split("\\.")[0]));
        }
        return outputListByFileName.iterator();
    })
    .mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);

    // stage 1 completed

    // starting stage 2

    //set the output folder
    stage1output.saveAsTextFile("outfile");
    //stop spark
  }

}
