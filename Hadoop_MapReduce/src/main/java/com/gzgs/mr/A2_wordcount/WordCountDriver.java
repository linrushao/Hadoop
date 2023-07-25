package com.gzgs.mr.A2_wordcount;

import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
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
        //设置提交的队列
        conf.set("mapreduce.job.queuename","linrushao");

//        //设置HDFS NameNode的地址
//        conf.set("fs.defaultFS","hdfs://hadoop201:8020");
//        //指定MapReduce运行在Yarn上
//        conf.set("mapreduce.framework.name","yarn");
//        //指定mapreduce可以再远程集群运行
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        //指定Yarn resourcemanager的位置
//        conf.set("yarn.resourcemanager.hostname","hadoop202");

        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(WordCountDriver.class);
//        job.setJar("D:\\CODE\\JavaCODE\\30_Hadoop_MapReduce\\target\\30_Hadoop_MapReduce-1.0-SNAPSHOT.jar");
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
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //8.提交Job
        job.waitForCompletion(true);
    }
}
