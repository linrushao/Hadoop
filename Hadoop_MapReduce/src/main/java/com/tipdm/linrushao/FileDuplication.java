package com.tipdm.linrushao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author linrushao
 * @Date 2023-07-26
 */
public class FileDuplication {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://master:8020");
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(FileDuplication.class);
        //关联Map和Reduce类
        job.setMapperClass(FD_Mapper.class);
        job.setReducerClass(FD_Reduce.class);
        //设置Reduce输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入和输出路径
        Path inpath = new Path("D:\\桌面\\day726\\数据");
        FileInputFormat.addInputPath(job, inpath);
        Path outPath = new Path("D:\\桌面\\day726\\数据\\output");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FD_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //定义Hadoop类型的word
            Text wordtext = new Text("");
            context.write(value, wordtext);
        }
    }

    public static class FD_Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }
}

