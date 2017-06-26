package com.renatoramos.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class WordCount {

    private Map<String, Integer> words;

    public Map<String, Integer> getWords() {
        return this.words;
    }

    public WordCount() {
        this.words = new HashMap<>();
    }

    public void run(String inputFilePath) {

        String masterUrl = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(WordCount.class.getName())
                .set("spark.driver.memory","4G")
                .set("spark.executor.memory","4G")
                .setMaster(masterUrl);


        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String,Integer> wordsRdd = context.textFile(inputFilePath)
                .flatMap(text -> Arrays.asList(text.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        this.words = wordsRdd.collectAsMap();

        context.close();
    }
}