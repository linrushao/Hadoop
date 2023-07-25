package com.gzgs.mr.A8_comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    private FlowBean outk = new FlowBean();
    private Text outv = new Text();

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

        String flowMsg =  value.toString();
        String[] split = flowMsg.split("\t");
        //1	13803847291	192.12.13.1	www.atguigu.com	1232	1231	200
        //封装key
        outk.setUpFlow(Long.parseLong(split[split.length-3]));
        outk.setDownFlow(Long.parseLong(split[split.length-2]));
        outk.setSumFlow();

        //2.封装value
        outv.set(split[1]);

        //3.写出
        context.write(outk,outv);



    }

}
