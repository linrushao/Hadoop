package com.gzgs.mr.A3_wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * KEYIN:默认情况下，是mr框架所读到的一行文本的起始偏移量，类型：long
 * 但是在Hadoop中有更精简的序列化接口，所以不直接用long ，而是用Longwriterable
 * VALUEIN：默认情况下，是mr框架所读的一行文本的内容， 类型：String  ,同上，用Text(import org.apache.hadoop.io.Text)
 *
 * KEYOUT:    是用户自定义逻辑处理完成之后输出数据的key,在此处是单词，类型 String  同上，用Text
 * VALUEOUT:  是用户自定义逻辑处理完成之后输出数据的value,在此处是单词次数，类型 Integer 同上，用 Intwriterable
 */

/*public class WordcountMapper  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
}*/
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
     * map 阶段的业务逻辑就写在自定义的map方法中
     * maptask会对每一行数据调用一次我们自定义的map方法
     *
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将maptask传给我们的内容先转换成String
        String line = value.toString();

        //根据空格这一行切成单词
        String[] words = line.split(" ");

        //将单词输出为<单词，1>
        for (String word : words) {
            //将单词作为key,将次数1作为value，以便后续的数据分发，可以根据单词分发，以便相同的单词会到相同reduce task
            context.write(new Text(word), new IntWritable(1));
        }
    }

}
