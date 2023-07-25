package edu.gzgs.linrushao.knnModel;

import java.io.IOException;

import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
	所谓的KNN 其实就是去计算一个待测样本跟多个已知类别的样本之间的距离，
	然后找出距离最近的k个已知类别的样本，从样本类别的多数类来给未知的待测样本进行预测
 */

public class MovieClassify{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//配置参数
		Configuration conf=new Configuration();
		FileSystem fs = FileSystem.get(conf);

		//配置任务
		Job job=Job.getInstance(conf, "movie_knn");
		job.setJarByClass(MovieClassify.class);//设置主类
		job.setMapperClass(MovieClassifyMapper.class);//设置Mapper类
		job.setReducerClass(MovieClassifyReducer.class);//设置Reducer类
		//设置输出键值对
		job.setMapOutputKeyClass(Text.class);//设置Mapper输出的键类型
		job.setMapOutputValueClass(DistanceAndLabel.class);//设置Mapper输出的值类型
		job.setOutputKeyClass(Text.class);//设置Reducer输出的键类型
		job.setOutputValueClass(NullWritable.class);//设置Reducer输出的值类型
		//设置输入输出路径
		Path input = new Path(Contant.TRAIN_PATH);
		FileInputFormat.addInputPath(job, input);//设置输入路径
		Path output = new Path(Contant.PREDICT_PATH);
		if (fs.exists(output)) {
			fs.delete(output, true);//删除输出路径
		}
		FileOutputFormat.setOutputPath(job, output);//设置输出路径
		System.exit(job.waitForCompletion(true)?0:1);//提交任务
		fs.close();
	}
}
