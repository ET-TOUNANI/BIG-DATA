package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ex2 {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        //sparkConf.setAppName("Exercice 2").setMaster("local[*]");spark://ETTOUNANI.:7077
        sparkConf.setAppName("Exercice 2").setMaster("spark://ETTOUNANI.:7077");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD1=sc.textFile("src/main/resources/ventes.txt");
        JavaRDD<List<String>> javaRDD2=javaRDD1.map(a->Arrays.asList(a.split(" ")));
        JavaPairRDD<String,Double> javaRDD3=javaRDD2.mapToPair(a->Tuple2.apply(a.get(1)+" "+(a.get(0).split("/"))[2],Double.valueOf(a.get(3))));
        JavaPairRDD<String,Double> javaRDD4=javaRDD3.reduceByKey((a,b)->a+b);
        List<Tuple2<String,Double>> jd=javaRDD4.collect();
        System.out.println("le total des ventes par ville pour une année donnée : ");
        for (Tuple2<String,Double> j:jd) {
            System.out.println(j._1+" "+j._2);
        }
        sc.close();
    }
}
