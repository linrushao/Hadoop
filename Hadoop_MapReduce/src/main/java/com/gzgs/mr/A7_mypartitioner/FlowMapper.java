package com.gzgs.mr.A7_mypartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    //定义写出的key
    private Text outk = new Text();
    //定义写出的value
    private FlowBean outv = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将读取到的一行数据转回String
        //1   124142 413.1.2134.232.31 www.chfia.com 1334 2132 200
        String line = value.toString();

        //2.切割数据
        String[] flowMsg = line.split("\t");

        //3.封装key
        outk.set(flowMsg[1]);

        //4.封装value
        outv.setUpFlow(Long.parseLong(flowMsg[flowMsg.length-3]));
        outv.setDownFlow(Long.parseLong(flowMsg[flowMsg.length-2]));
        outv.setSumFlow();

        //5.写出
        context.write(outk,outv);
    }
}
