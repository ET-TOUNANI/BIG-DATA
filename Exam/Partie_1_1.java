package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class Solution {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss = SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://10.233.0.55:3306/DB_AEROPORT");
        options.put("user", "ETTOUNANI");
        options.put("password", "tounani2001.");
        // Charger la table des vols comme DataFrame
        Dataset<Row> volsDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "VOLS").load();

        // Charger la table des passagers comme DataFrame
        Dataset<Row> passagerDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "passagers").load();
        // Charger la table des réservations comme DataFrame
        Dataset<Row> reservationDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "reservations").load();


        // Joindre les tables pour obtenir le nombre de passagers par vol
        Dataset<Row> nbrPassengersPerFlight = reservationDF.join(volsDF, reservationDF.col("id_vol").equalTo(volsDF.col("id")))
                .join(passagerDF, reservationDF.col("id_passager").equalTo(passagerDF.col("id")))
                .groupBy(volsDF.col("id"), volsDF.col("date_depart"))
                .agg(countDistinct(passagerDF.col("id")).as("NOMBRE"))
                .select(volsDF.col("id").as("ID_VOL"), volsDF.col("date_Depart").as("DATE_DEPART"), col("NOMBRE"));

        // Afficher les résultats
        nbrPassengersPerFlight.show();

    }

}