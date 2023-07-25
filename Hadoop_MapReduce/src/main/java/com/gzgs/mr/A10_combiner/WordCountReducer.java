package com.gzgs.mr.A10_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Reducer类需要继承Hadoop提供的Reducer类，指定4个泛型，重写reduce方法
 * 4个泛型（基于wordcount分析）
 * KEYIN:输入数据的key的类型        Text（对应Mapper输出的key的类型）表示一个单词
 * VALUEIN:输入数据的value的类型    IntWritable（对应Mapper输出的value的类型）表示单词出现1次
 *
 * KEYOUT:输出数据的key的类型           Text,表示一个单词
 * VALUEOUT:输出数据的value的类型       IntWritable，表示某个单词出现的总次数
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    //定义输出的v
    private IntWritable outv = new IntWritable();

    /**
     * reduce方式是mr中reduce阶段的核心处理过程。
     * 多个相同key的kv对，会组成一组kv，一组相同的多个kv对会执行一次reduce方法
     * @param key 输入数据的key表示一个单词
     * @param values  当前key对应的所有value
     * @param context  上下文对象，负责调度整个Reduce类中的方法的执行
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        //1.迭代values，取出每个value，进行汇总
        int sum = 0;
        for(IntWritable value:values){
            sum += value.get();
        }

        //2.封装value
        outv.set(sum);

        //3.写出
        context.write(key,outv);
    }

}
