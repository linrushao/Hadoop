package com.gzgs.mr.A12_outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        //http://www.xxx.com
        //写出
        context.write(value,NullWritable.get());
    }


}
