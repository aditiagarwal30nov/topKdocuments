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
        System.out.println(numOfDocs);
        List<String> stopWords = sc.textFile("stopwords.txt").collect();
        List<String> query = sc.textFile("query.txt").collect();
        List<String> queryWords = Arrays.asList(query.get(0).split(" "));


        //stage 1 starting
        JavaPairRDD<String, Integer> stage1output = textFiles
                .flatMap(s -> {
                    String replacedLineEndings = s._2.replace("\n", " ").replace("\r", " ");
                    List<String> inputListByFileName = Arrays.asList(replacedLineEndings.split(" "));
                    ArrayList<String> outputListByFileName = new ArrayList<String>();
                    Path p = Paths.get(s._1);
                    String filename = p.getFileName().toString();
                    for (String str : inputListByFileName) {
                        str = str.replaceAll("[^a-zA-Z0-9']", "").trim().toLowerCase();
                        if (!stopWords.contains(str) && str.length() > 0)
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
                    ArrayList<Tuple2<String, Double>> tfIdfVector = new ArrayList<Tuple2<String, Double>>();
                    int numberOfOccurences = Iterables.size(s._2);
                    for (Tuple2<String, Integer> tuple : s._2) {
                        double tfIdfValue = calculateTfIdf(numOfDocs, tuple._2, numberOfOccurences);
                        tfIdfVector.add(new Tuple2<>(tuple._1, tfIdfValue));
                    }
                    return new Tuple2<>(s._1, tfIdfVector);
                })
                .flatMapValues(s -> Lists.newArrayList(s))
                .mapToPair(s -> {
                    String key = s._1 + "@" + s._2._1;
                    return new Tuple2<>(key, s._2._2);
                });

        // stage 2 complete

        // starting stage 3
        JavaPairRDD<String, Double> stage3output = stage2output.mapToPair(
                s -> {
                    String[] word = s._1.split("@");
                    return new Tuple2<>(word[1], new Tuple2<>(word[0], s._2));
                })
                .groupByKey()
                .mapToPair(s -> {
                    ArrayList<Tuple2<String, Double>> normalizedTfIdfVector = new ArrayList<Tuple2<String, Double>>();
                    double sumOfSquares = 0.0;
                    long numberOfOccurences = Iterables.size(s._2);
                    for (Tuple2<String, Double> tuple : s._2) {
                        sumOfSquares += (tuple._2) * (tuple._2);
                    }
                    for (Tuple2<String, Double> tuple : s._2) {
                        double normalizedtfIdfValue = calculateNormalizedTfIdf(sumOfSquares, tuple._2);
                        normalizedTfIdfVector.add(new Tuple2<>(tuple._1, normalizedtfIdfValue));
                    }
                    return new Tuple2<>(s._1, normalizedTfIdfVector);
                })
                .flatMapValues(s -> Lists.newArrayList(s))
                .mapToPair(s -> {
                    String key = s._2._1 + "@" + s._1;
                    return new Tuple2<>(key, s._2._2);
                });
        //set the output folder

        // starting stage 4
        JavaPairRDD<String, Double> stage4output = stage3output.mapToPair(
                s -> {
                    String[] word = s._1.split("@");
                    return new Tuple2<>(word[1], new Tuple2<>(word[0], s._2));
                })
                .filter(s -> queryWords.contains(s._2._1))
                .mapToPair(s -> {
                    return new Tuple2<>(s._1, s._2._2);
                })
                .reduceByKey((a, b) -> (a + b));

        // stage 4 complete

        // starting stage 5
        JavaPairRDD<Double, String> stage5output = stage4output.mapToPair(s -> new Tuple2<>(s._2, s._1)).sortByKey(false);

        stage5output.saveAsTextFile("outfile");
        //stop spark
    }

    private static double calculateNormalizedTfIdf(Double sumOfSquares, double freq) {
        return (freq / Math.sqrt(sumOfSquares));
    }

    private static double calculateTfIdf(long numOfDocs, Integer freq, int numberOfOccurences) {
        double v = (1 + Math.log(freq));
        double u = (Math.log(1.000 * numOfDocs / numberOfOccurences));
        return v * u;
    }

}
