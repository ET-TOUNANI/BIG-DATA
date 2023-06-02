package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Avions2 {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("StreamProcessing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));
        JavaDStream<String> dStream = streamingContext.textFileStream("hdfs://localhost:9000/input");

        // Afficher les deux mois de l'année en cours avec le moins d'incidents
        JavaDStream<List<String>> avionsLigne = dStream.map(line -> Arrays.asList(line.split("\n")));
        JavaDStream<List<String>> avions = avionsLigne.map(line -> Arrays.asList(line.get(0).split(",")));

        // Compter le nombre d'incidents par mois
        JavaPairDStream<String, Integer> moisIncidentsCount = avions
                .mapToPair(avion -> {
                    String dateStr = avion.get(3);
                    LocalDateTime dateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    String mois = dateTime.getMonth().toString();
                    return new Tuple2<>(mois, 1);
                })
                .reduceByKey(Integer::sum);

        // Trouver les deux mois avec le moins d'incidents
        JavaDStream<Tuple2<String, Integer>> deuxMoisMoinsIncidents = moisIncidentsCount
                .reduce((mois1, mois2) -> mois1._2() < mois2._2() ? mois1 : mois2);

        // Afficher les deux mois avec le moins d'incidents
        deuxMoisMoinsIncidents.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
