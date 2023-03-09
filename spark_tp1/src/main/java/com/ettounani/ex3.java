package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class ex3 {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("Exercice 3").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD1=sc.textFile("src/main/resources/2020.csv");

        List<String> data=javaRDD1.collect();
        for (String d:
             data) {
            System.out.println(d);
        }
    }
}
