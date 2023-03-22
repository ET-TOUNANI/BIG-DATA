package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Ventes {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("StreamProcessingVente").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(2000));
        JavaDStream<String> dStream = streamingContext.textFileStream("hdfs://localhost:9090/FilesRes");
        JavaDStream<List<String>> ventes = dStream.map(line -> Arrays.asList(line.split(" ")));
        JavaPairDStream<String, Double> pairs = ventes.mapToPair((vnt -> new Tuple2<>(vnt.get(0) + " " + vnt.get(1), Double.parseDouble(vnt.get(3)))));
        JavaPairDStream<String, Double> ventesCount = pairs.reduceByKey((x, y) -> x + y);
        ventesCount.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
