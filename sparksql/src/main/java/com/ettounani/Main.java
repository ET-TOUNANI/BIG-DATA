package com.ettounani;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss= SparkSession.builder().appName("sql").master("local[*]").getOrCreate();
        Dataset<Row> emp=ss.read().option("multiline",true).json("data.json");
        emp.select("departement","salary").groupBy("departement").avg("salary").show();



    }
}