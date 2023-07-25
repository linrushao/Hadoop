package edu.gzgs.linrushao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    //自定义TokenizerMapper类，继承Mapper类，同时需要设置输入键值对格式（与输入格式设置的类读取生成的键值对格式匹配），
    // 设置输出键值对格式（与Driver中设置的Mapper输出键值对格式匹配）
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //setup函数，初始化工作

        //map函数，针对每条输入键值对执行函数中定义的逻辑处理，并按规定的键值对格式输出
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //把每个输入键值对（键值对组成为<行的偏移量，行字符串>）的值（行字符串）按照分隔符进行分隔，
            // 得到每个单词
            StringTokenizer itr = new StringTokenizer(value.toString());

            //输出每个单词和1组成的键值对
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
        //cleanup函数，关闭资源
    }

    //自定义IntSumReducer类，继承Reducer类，
    // 同时需要设置输入键值对格式（与Mapper的输出键值对格式匹配），设置输出键值对格式（与Driver中设置的Reducer输出键值对格式匹配）

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        //setup函数，初始化工作

        //reduce函数，针对每条输入键值对执行函数中定义的逻辑处理，并按规定的键值对格式输出
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //相同的键，将其列表值全部累加起来
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            //输出结果键值对
            context.write(key, result);
        }
        //cleanup函数，关闭资源
    }

    public static void main(String[] args) throws Exception {
        //初始化相关hadoop配置
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        //新建job
        Job job = Job.getInstance(conf, "word count");
        //设置主类
        job.setJarByClass(WordCount.class);
        //必须设置
        job.setMapperClass(TokenizerMapper.class);
        //可选设置
        job.setCombinerClass(IntSumReducer.class);
        //必须设置,与Combiner相同
        job.setReducerClass(IntSumReducer.class);

        //设置输出键值对格式,当Mapper输出键值对格式与Reducer输入键值对格式一致时，
        // 可以只设置Reducer输出键值对格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入路径
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        //提交任务并等待
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
