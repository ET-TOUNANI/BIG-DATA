package com.ettounani;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ex1 {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("Exercice 1").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD1=sc.parallelize(Arrays.asList("Abderrahmane","Boutaina","ettounani","oubella","youssef","khadija","oumaima","ahmed"));
        JavaRDD<Integer> javaRDD2=javaRDD1.flatMap(a->(Arrays.asList(a.length())).iterator());
        JavaRDD<Integer> javaRDD3=javaRDD2.filter((a)->{
            return a > 5;
        });
        JavaRDD<Integer> javaRDD4=javaRDD2.filter((a)->{
            return a > 3;
        });
        JavaRDD<Integer> javaRDD5=javaRDD2.filter((a)->{
            return a > 7;
        });
        JavaRDD<Integer> javaRDD6=javaRDD4.union(javaRDD3);
        JavaRDD<Integer> javaRDD71=javaRDD5.map(a->a+1);
        JavaRDD<Integer> javaRDD81=javaRDD6.map(a->a-1);

        JavaPairRDD<String,Integer>javaRDD7=javaRDD1.mapToPair(nm-> Tuple2.apply(nm,nm.length()));

        JavaRDD<Integer> javaRDD7g=javaRDD5.map(a->a+1);
        JavaRDD<Integer> javaRDD8=javaRDD6.map(a->a-1);








        List<Double> notes=javaRDD3.collect();
        for(double n:notes){
            System.out.println(n);
        }
    }
}