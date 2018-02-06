package com.mycompany.app;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("TopKMostRelevantDocuments").set("spark.files.overwrite", "true"));

        //set the input files
        JavaPairRDD<String, String> textFiles = sc.wholeTextFiles("datafiles");
        long numOfDocs = textFiles.keys().count();
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
                        if (!stopWords.contains(str))
                            outputListByFileName.add(str + "@" + (filename.split("\\.")[0]));
                    }
                    return outputListByFileName.iterator();
                })
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        // stage 1 completed

        // starting stage 2
        JavaPairRDD<String, Double> stage2output = stage1output.mapToPair(
                s -> {
                    String[] word = s._1.split("@");
                    Tuple2<String, Integer> fileAndFreq = new Tuple2<>(word[1], s._2);
                    return new Tuple2<>(word[0], fileAndFreq);
                }).groupByKey()
                .mapToPair(s -> {
                    ArrayList<Tuple2<String, Double>> outputListByFileName = new ArrayList<Tuple2<String, Double>>();
                    long numberOfOccurences = Iterables.size(s._2);
                    for (Tuple2<String, Integer> tuple : s._2) {
                        double tfIdfValue = calculateTfIdf(numOfDocs, tuple._2, numberOfOccurences);
                        outputListByFileName.add(new Tuple2<>(tuple._1, tfIdfValue));
                    }
                    return new Tuple2<>(s._1, outputListByFileName);
                })
                .flatMapValues(s -> Lists.newArrayList(s))
                .mapToPair(s -> {
                    String key = s._1 + "@" + s._2._1;
                    return new Tuple2<>(key, s._2._2);
                });

        // stage 2 complete
        //set the output folder
        stage2output.saveAsTextFile("outfile");
        //stop spark
    }

    private static double calculateTfIdf(long numOfDocs, Integer freq, long numberOfOccurences) {
        return 1 + Math.log(freq) * Math.log(numOfDocs / numberOfOccurences);
    }

}
