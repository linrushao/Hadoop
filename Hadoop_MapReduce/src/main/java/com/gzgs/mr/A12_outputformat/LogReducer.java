package com.gzgs.mr.A12_outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, NullWritable, Text,NullWritable> {

    @Override
    protected void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        //迭代写出
        for(NullWritable value:values){
            context.write(key,value);
        }
    }

}
