package edu.gzgs.linrushao.dataJoin;
import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 * @Author LinRuShao
 * @Date 2023/5/26 11:27
 */
/*
 * 根据UserID（用户ID）字段连接ratings.dat数据和users.dat数据
 * 连接结果得到一份包含UserID（用户ID），Gender（性别），MovieID（电影ID）的数据
 * */
public class UserAddRating {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        实例化一个Configuration对象，获取集群的相关配置
        Configuration conf = new Configuration();
//        初始化Job任务
        Job job = Job.getInstance(conf);
//        设置主类名称
        job.setJarByClass(UserAddRating.class);
        job.setMapperClass(UARMapper.class);
//        mapper输出的值的类型与默认类型不同时，需要通过以下语句设置输出值的类型
//        Map默认的输出键的类型是Text类型
        job.setMapOutputKeyClass(Text.class);
//        Map默认的输出值的类型是IntWritable类型
        job.setMapOutputValueClass(NullWritable.class);
//        设置不需要任何的Reducer阶段
        job.setNumReduceTasks(0);
//        设置最终MapReduce的键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        设置程序的输入与输出路径
        FileInputFormat.addInputPath(job,new Path(Contant.RATINGS));
        FileOutputFormat.setOutputPath(job,new Path(Contant.USER_AND_RATING_PATH));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
