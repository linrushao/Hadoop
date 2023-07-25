package edu.gzgs.linrushao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author LinRuShao
 * @Date 2022/11/2 10:45
 */
public class DataTwoSort {
    public static class NewKey2 implements WritableComparable<NewKey2> {
        Long first;
        Long second;
        public NewKey2() {}
        public NewKey2(Long first, Long second) {
            this.first = first;
            this.second = second;
        }
        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readLong();
            this.second = in.readLong();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.first);
            out.writeLong(this.second);
        }
        @Override
        public int compareTo(NewKey2 o) {
            if (this.first != o.first) {
                return (int) (this.first - o.second);
            } else if (this.second != o.second) {
                return (int) (this.second - o.second);
            } else {
                return 0;
            }
        }
    }

    public static class MyGroupComparator implements RawComparator<NewKey2> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(NewKey2 o1, NewKey2 o2) {
            return (int) (o1.first - o2.first);
        }
    }

    public static class MyMap extends Mapper<LongWritable, Text, NewKey2, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, NewKey2, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String[] vals = value.toString().split("\t");
            NewKey2 k2 = new NewKey2(Long.parseLong(vals[0]), Long.parseLong(vals[1]));
            LongWritable v2 = new LongWritable(Long.parseLong(vals[1]));
            context.write(k2, v2);
        }
    }

    public static class MyReduce extends Reducer<NewKey2, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(NewKey2 key,
                              Iterable<LongWritable> values,
                              Reducer<NewKey2, LongWritable, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            long max = Long.MIN_VALUE;
            for (LongWritable v2 : values) {
                if (v2.get() > max) {
                    max = v2.get();
                    context.write(new LongWritable(key.first), new LongWritable(max));
                }
            }
        }
    }

    public static final String INPUT_PATH = "hdfs://master:8020/input/data";
    public static final String OUTPUT_PATH = "hdfs://master:8020/output/twosort";

    public static void main(String[] args) throws Exception {
        /**
         * 在windows客户端命名，否则在hdfs中没有权限
         */
//        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
//        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = Job.getInstance(conf, "sort data two");
        job.setJarByClass(DataTwoSort.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MyMap.class);
        job.setMapOutputKeyClass(NewKey2.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);
    }
}
