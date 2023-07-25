package com.gzgs.mr.A4_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 * 为啥要main方法：先启动自己，然后把整个程序提交到集群
 * 相当于yarn集群的客户端，yarn去分配运算资源，然后才能启动
 * 以为是yarn的客户端，所以要再次封装mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 */
public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*
         * import org.apache.hadoop.mapreduce.xxxx
         * import org.apache.hadoop.mapred.xxx  mapred是老版本的，我们用MapReduce
         */
        Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname","hadoop01");*/
        //给一些默认的参数
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径  把jar包提交到yarn
        job.setJarByClass(WordcountDriver.class);

        /*
         * 告诉框架调用哪个类
         * 指定本业务job要是用的mapper/Reducer业务类
         */
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReduce.class);

        /*
         * 指定mapper输出数据KV类型
         */
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终的输出数据的kv类型  ，有时候不需要reduce过程，如果有的话最终输出指的就是指reducekv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定job的文件输入的原始目录
        //paths指你的待处理文件可以在多个目录里边
        //第一个参数是你给那个job设置  后边的参数 逗号分隔的多个路径 路径是在hdfs里的
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 指定job 的输出结果所在的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * 找yarn通信
         * 将job中配置的参数，  以及job所用的java类所在的jar包提交给yarn去运行
         */
        /*job.submit();*/
        // 参数表示程序执行完，告诉我们是否执行成功
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}