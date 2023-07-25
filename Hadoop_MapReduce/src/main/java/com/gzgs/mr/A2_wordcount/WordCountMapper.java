package com.gzgs.mr.A2_wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 编写一个MR程序，通常都需要分三步
 * 1.编写Mapper
 * 2.编写Reducer
 * 3.编写Driver
 *
 * 插件类型的开发套路：
 * 1.继承类或者实现接口
 * 2.实现或者重写相关的方法
 * 3.提交执行
 *
 *自定义Mapper的开发
 * 继承Hadoop提供的Mapper类，提供输入和输出kv的类型，，并重写map的方法
 *
 * Mapper类的四个泛型(以wordcount来分析)：
 * KEYIN:输入数据的key的类型        LongWritable, 用来表示偏移量（从文件的那个位置读取数据）
 * VALUEIN:输入数据的value的类型    Text，从文件中读取到的一行数据
 *
 * KEYOUT:输出数据的key的类型           Text,表示一个单词
 * VALUEOUT:输出数据的value的类型       IntWritable，表示这个单词出现1次
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    //定义输出的k
    private Text outk = new Text();
    //定义输出的v
    private IntWritable outv = new IntWritable(1);
    /**
     * map方法是整个mr中map阶段的核心处理方法
     * 每读取一行数据，都要执行一次map方法
     * key  表示偏移量
     * value  读取到一行数据
     * context  上下文对象，负责调度整个Mapper类中的方法的执行
     */
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        //1.将读取到的一行数据从text转回String(方便操作)
        //例如：linrushao  linrushao
        String line = value.toString();
        //2.按照分隔符分割当前的数据
        //[linrushao,linrushao]
        String[] words = line.split(" ");
        //3.将word进行迭代处理，把迭代出的每个单词拼成kv写出
        for(String word:words){
            //封装输出的k
            outk.set(word);
            //写出
            context.write(outk,outv);
        }
    }
}
