package com.gzgs.mr.A10_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text,IntWritable,Text, IntWritable> {

    private IntWritable outv = new IntWritable();

    @Override
    protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value :values){
            sum += value.get();
        }

        outv.set(sum);
        //写出
        context.write(key,outv);
    }
}
