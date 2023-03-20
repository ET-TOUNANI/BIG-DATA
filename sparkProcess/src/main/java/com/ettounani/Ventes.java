package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Ventes {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("StreamProcessingVente").setMaster("local[*]");
        JavaStreamingContext streamingContext=new JavaStreamingContext(conf,new Duration(2000));
        JavaReceiverInputDStream<String> dStream=streamingContext.socketTextStream("localhost",9999);
        JavaDStream<String> words=dStream.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairs=words.mapToPair((word->new Tuple2<>(word,1)));
        JavaPairDStream<String,Integer>ventesCount=pairs.reduceByKey((x,y)->x+y);
        ventesCount.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
