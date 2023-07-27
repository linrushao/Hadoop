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
public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:8020");
        Job job = Job.getInstance(conf);

        //设置主类
        job.setJarByClass(WordCount.class);
        //关联Map和Reduce类
        job.setMapperClass(WC_Mapper.class);
        job.setReducerClass(WC_Reduce.class);
        //设置Reduce输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        Path inpath = new Path("hdfs://master:8020/words.txt");
        FileInputFormat.addInputPath(job, inpath);
        Path outPath = new Path("hdfs://master:8020/output");
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WC_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //定义Hadoop类型的word
            Text wordtext = new Text();
            //单词计数中的1
            IntWritable one = new IntWritable(1);
            //对每一行进行分割
            String[] words = value.toString().split(" ");
            for (String word : words) {
                wordtext.set(word);
                context.write(wordtext, one);
            }
        }
    }

    public static class WC_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

}
