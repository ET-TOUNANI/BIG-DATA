package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;


public class Solution2 {
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

        // Filtrer les vols en cours en comparant la date de départ avec la date actuelle
        LocalDate currentDate = LocalDate.now();
        Dataset<Row> volsEnCoursDF = volsDF.filter(functions.col("date_Depart").gt(currentDate.toString()));

        // Sélectionner les colonnes requises et afficher les résultats
        Dataset<Row> volsEnCoursDisplay = volsEnCoursDF.select("id", "date_Depart", "date_arrive");
        volsEnCoursDisplay.show();
    }
}