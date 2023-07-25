package com.gzgs.mr.A3_wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * KEYIN, VALUEIN, 对应mapper的KEYOUT,VALUEOUT  类型对应
 *
 * KEYOUT, VALUEOUT,是自定义reduce逻辑处理结果的输出数据类型
 *
 * KEYOUT是单词
 * VALUEOUT是总次数
 */
/*public class WordcountReduce extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
}*/
public class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    /*
     * 入参key,是一组相同单词kv对的key
     * <dayu,1><dayu,1><dayu,1><dayu,2>
     * <hello,1><hello,1><hello,1><hello,1>
     * <world,2><world,2><world,2><world,2>
     * 上面三个分为三组，每组都调用一次reduce方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable value:values) {
            count+=value.get();
        }
        //每组的统计结果
        context.write(key, new IntWritable(count));
    }
}