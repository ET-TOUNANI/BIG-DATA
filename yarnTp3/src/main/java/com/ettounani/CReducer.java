package com.ettounani;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CReducer extends Reducer<Text, FloatWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> it=values.iterator();
        float min = it.next().get();
        float max = min;

        while (it.hasNext()){
            float value = it.next().get();
            if(value<min) min = value;
            if(value>max) max = value;
        }
        context.write(key,new Text(min +" "+ max));
    }
}
