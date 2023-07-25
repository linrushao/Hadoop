package com.gzgs.mr.A9_combineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 驱动类，主要将我们写好的mr封装成一个job对象，进行提交，然后执行
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(WordCountDriver.class);
        //4.关联Mapper和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //5.设置mapper输出的key和value的类型
        job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //6.设置最终输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //7.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Atest\\inputWord"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Atest\\outinput"));
        //设置InputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);//4M
        //8.提交Job
        job.waitForCompletion(true);
    }
}
