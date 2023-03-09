package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ex3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Exercice 3").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = sparkContext.textFile("src/main/resources/2020.csv");
        JavaRDD<List<String>> javaRDD2 = javaRDD.map(a -> Arrays.asList(a.split(",")));
        JavaPairRDD<Integer, String> javaPairRDD = javaRDD2.mapToPair((n) -> Tuple2.apply(Integer.parseInt(n.get(3)), n.get(2)));
        JavaPairRDD<Integer, String> javaPairRDD1 = javaPairRDD.sortByKey(false);// change false to true if u want the max

        List<Tuple2<Integer, String>> data = javaPairRDD1.collect();
        System.out.println(data.get(0)._1 + " " + data.get(0)._2); // print the min
        sparkContext.close();
    }
}
