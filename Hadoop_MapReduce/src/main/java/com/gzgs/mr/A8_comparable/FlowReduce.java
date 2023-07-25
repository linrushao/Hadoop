package com.gzgs.mr.A8_comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<FlowBean, Text,Text ,FlowBean> {

    //疑问：两个自定义的对象如何比较是否相同
    @Override
    protected void reduce(FlowBean key,Iterable<Text> values,Context context) throws IOException, InterruptedException {

        for(Text value : values){

            //写出
            context.write(value,key);

        }

    }
}
