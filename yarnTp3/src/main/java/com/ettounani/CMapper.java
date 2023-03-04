package com.ettounani;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CMapper extends Mapper<LongWritable, Text,Text,FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        String Departements[]= value.toString().split(",");
        context.write(new Text(Departements[2]),new FloatWritable(Float.parseFloat(Departements[4])));
    }
}
