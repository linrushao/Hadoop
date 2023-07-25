package edu.gzgs.linrushao.dataCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	map端会有分组的步骤，会将键相同的值做一个分组处
	对每个用户看过电影类型进行统计
	并对Gender（性别）做一步转换，如果是女性（F）则用0标记，如果是男性（M）则用1标记
 */

public class MovieGenresMapper extends Mapper<LongWritable, Text, Text, Text>{
	Text user_gender = new Text();
	Text genres = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 分割每一条数据：userid：：性别：：电影类型
		String[] vals = value.toString().split("::");
		// 性别转换
		if(vals[1].equals("F")) {
			user_gender.set(vals[0]+"::"+0);
		}
		else {
			user_gender.set(vals[0]+"::"+1);
		}

		genres.set(vals[2]);
		context.write(user_gender, genres);
		//输出键值对[(userid,gender),movieType]
		//输出键值对[(用户id,性别),电影类型]
	}

}
