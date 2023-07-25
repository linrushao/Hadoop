package com.gzgs.mr.A14_mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MapJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.创建Job对象
        Job job = Job.getInstance(conf);
        //3.关联驱动类
        job.setJarByClass(MapJoinDriver.class);
        //4.关联Mapper
        job.setMapperClass(MapJoinMapper.class);
        //5.设置map输出的k和v的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6.设置最终输出的k和v的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //7.设置输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("D:\\Atest\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\Atest\\outputtable2"));

        //设置reduce
        job.setNumReduceTasks(0);


        //缓存数据
        job.addCacheFile(URI.create("file:///D:/Atest/inputtable3/pd.txt"));
        //8.提交
        job.waitForCompletion(true);

    }
}
