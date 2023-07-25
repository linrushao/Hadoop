package com.gzgs.mr.A12_outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(LogDriver.class);
        //4.关联Mapper和Reducer类
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        //5.设置map输出的k和v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6.设置最终输出的k和v的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //7.设置输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Atest\\inputlog"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\Atest\\outinputlog"));

        //设置自定义的OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);
        //8.提交
        job.waitForCompletion(true);
    }
}
