package com.gzgs.mr.A11_group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(OrderDriver.class);
        //4.关联Mapper和Reducer类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        //5.设置mapper输出的key和value的类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6.设置最终输出的key和value的类型
        job.setOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //7.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Atest\\inputWord"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Atest\\outinput"));

        //设置分组比较器
        job.setGroupingComparatorClass(OrderGroupComparator.class);
        //8.提交Job
        job.waitForCompletion(true);

    }

}
