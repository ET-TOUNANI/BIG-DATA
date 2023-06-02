package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Avions {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("StreamProcessing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));
        JavaDStream<String> dStream = streamingContext.textFileStream("hdfs://localhost:9000/input");

        // Afficher l'avion ayant le plus d'incidents
        JavaDStream<List<String>> avionsLigne = dStream.map(line -> Arrays.asList(line.split("\n")));
        JavaDStream<List<String>> avions = avionsLigne.map(line -> Arrays.asList(line.get(0).split(",")));

        // Compter le nombre d'incidents par avion
        JavaPairDStream<String, Integer> incidentsCount = avions
                .mapToPair(avion -> new Tuple2<>(avion.get(2), 1))
                .reduceByKey(Integer::sum);

        // Trouver l'avion avec le plus grand nombre d'incidents
        JavaDStream<Tuple2<String, Integer>> avionMaxIncidents = incidentsCount
                .reduce((avion1, avion2) -> avion1._2() > avion2._2() ? avion1 : avion2);

        // Afficher l'avion avec le plus grand nombre d'incidents

        avionMaxIncidents.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
