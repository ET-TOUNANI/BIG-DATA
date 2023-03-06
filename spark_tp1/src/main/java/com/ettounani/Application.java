package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class Application {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("TP 1 Spark").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<Double> javaRDD1=sc.parallelize(Arrays.asList(12.0,2.9,3.0,10.5,23.1));
        JavaRDD<Double> javaRDD2=javaRDD1.map((a) -> a+1);
        JavaRDD<Double> javaRDD3=javaRDD2.filter((a)->{
            return a > 10;
        });
        List<Double> notes=javaRDD3.collect();
        for(double n:notes){
            System.out.println(n);
        }
    }
}