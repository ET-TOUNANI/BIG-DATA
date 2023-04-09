package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Solution {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_HOPITAL");
        options.put("user","root");
        options.put("password","");

        /*
        Dataset<Row> dfEmp = ss.read().format("jdbc")
                .options(options)
                //.option("dbtable", "EMPLOYES")
                .option("query","select * from doctor")
                .load();
        dfEmp.printSchema();*/


        // question 1
        /*
        System.out.println("La premiere question Version SQL");
        Dataset<Row> nbrConsultationPerDay=ss.read().format("jdbc").options(options).option("query","SELECT date , COUNT(date) as \"le nombre de consultation\" FROM consultation GROUP BY(date)").load();
        nbrConsultationPerDay.show();

        System.out.println("La premiere question Version Methode");
        Dataset<Row> nbrConsultationPerDay2=ss.read().format("jdbc").options(options).option("dbtable", "consultation").load();
        nbrConsultationPerDay2.select("date").groupBy(col("date")).count().alias("le nombre de consultation").show();

        // question 2
        System.out.println("La deuxieme question Version SQL ");
        Dataset<Row> nbrConsultationPerDoctor=ss.read().format("jdbc").options(options).option("query","SELECT name,speciality,COUNT(c.doctor_id) as \"le nombre de consultation\" FROM consultation as c ,doctor  WHERE c.doctor_id=doctor.doctor_id GROUP BY(c.doctor_id)").load();
        nbrConsultationPerDoctor.show();
        System.out.println("La deuxieme question Version Methode");
        // Load the tables as DataFrames
        Dataset<Row>  consultDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "consultation").load();
        Dataset<Row> doctorDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "doctor").load();

// Join the tables based on the doctor ID
        Dataset<Row> resultDF = consultDF.join(doctorDF, "doctor_id");
//resultDF.show();
// Select the columns you want to keep
        Dataset<Row> nbrConsultationPerDoctor2 = resultDF.select("doctor_id","name","speciality")
                .groupBy("doctor_id","name","speciality").count().alias("le nombre de consultation");
        nbrConsultationPerDoctor2.select("name","speciality","count").show();
*/
        // question 3
        // Load the consultation table as a DataFrame
        Dataset<Row> consultationDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "consultation").load();

// Group by doctor_id and count the number of distinct patients per doctor
        Dataset<Row> nbrPatientsPerDoctor = consultationDF.groupBy("doctor_id")
                .agg(countDistinct("patient_id").as("nbr_patients"));

// Join with the doctor table to get the doctor's name
        Dataset<Row> doctorDF = ss.read().format("jdbc").options(options)
                .option("dbtable", "doctor").load();
        Dataset<Row> resultDF = nbrPatientsPerDoctor.join(doctorDF, "doctor_id");

// Select the columns you want to keep
        Dataset<Row> nbrPatientsPerDoctorByName = resultDF.select("name", "speciality", "nbr_patients");
        nbrPatientsPerDoctorByName.show();
    }
}
