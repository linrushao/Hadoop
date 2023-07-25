package edu.gzgs.linrushao.dataCount;

import edu.gzgs.linrushao.coreModel.Contant;
import edu.gzgs.linrushao.dataJoin.UserAddRatingAddMovie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
	对每个用户看过电影类型进行统计
	并对Gender（性别）做一步转换，如果是女性（F）则用0标记，如果是男性（M）则用1标记
 */

public class MovieGenres{

	public static void main(String[] args) throws Exception {
		// 定义获取集群配置的对象
		Configuration conf = new Configuration();
		//定义任务的对象
		Job job = Job.getInstance(conf);
		job.setJarByClass(MovieGenres.class); //Driver类
		job.setMapperClass(MovieGenresMapper.class); //Mapper类
		//Map输出键值对类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MovieGenresReducer.class); //Reducer类
		//Reduce输出键值对类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//输入路径
		Path input = new Path(Contant.USER_AND_RATING_MOVIE_PATH);
		FileInputFormat.addInputPath(job, input);
		//输出路径
		Path output = new Path(Contant.MOVIES_GENRES_COUNT_PATH);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		//提交任务
		System.exit(job.waitForCompletion(true)?0:1);
		fs.close();
	}

}
