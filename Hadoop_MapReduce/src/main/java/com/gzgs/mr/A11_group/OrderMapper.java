package com.gzgs.mr.A11_group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private OrderBean outk = new OrderBean();

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //100000001 pdt_01  222.3
        String[] orders = line.split("\t");
        //封装key
        outk.setOrderId(orders[0]);
        outk.setPrice(Double.parseDouble(orders[2]));

        //写出
        context.write(outk,NullWritable.get());
    }





}
