package com.gzgs.mr.A7_mypartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(FlowDriver.class);
        //4.关联Mapper和Reducer类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //5.设置map输出的k和v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //6.设置最终输出的k和v的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //7.设置输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Atest\\inputPhone"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\Atest\\outinputPhone"));
        //设置分区类
        job.setPartitionerClass(PhonePartitioner.class);
        job.setNumReduceTasks(5);
        //8.提交
        job.waitForCompletion(true);
    }
}
