package edu.gzgs.linrushao.dataCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
	 map端会有分组的步骤，会将键相同的值做一个分组处
	对每个用户看过电影类型进行统计
	并对Gender（性别）做一步转换，如果是女性（F）则用0标记，如果是男性（M）则用1标记
 */

public class MovieGenresReducer extends Reducer<Text, Text, Text, NullWritable>{
	Text result=new Text();

	//reduce的输入是键值对   <(用户id,性别),<(type1|type2|type3),(type1|type2),(type4)>>   每个类型里面有多个的话是用|分割的
	@Override
	protected void reduce(Text user_gender, Iterable<Text> movietypes, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
	//根据电影类型初始化一个值都为0的HashMap
	//将所有电影类型排成一个固定结构
	String[] types ={"Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"};
	Map<String,Integer> genres = new HashMap<String, Integer>();
	for(String type:types) {
		genres.put(type, 0);
	}

	// 遍历用户的所有电影类型，将每种类型的访问次数累加到一个HashMap
	for(Text movietype:movietypes) {
		String[] labels = movietype.toString().split("\\|");
		for(String label:labels) {
			if(genres.containsKey(label)) {
				genres.put(label, genres.get(label)+1);
			}
		}
	}
	//定义一个StringBuffer，用于存储所有需要拼接的值
	//在使用 StringBuffer类时，每次都会对 StringBuffer对象本身进行操作，而不是生成新的对象，所以如果需要对字符串进行修改推荐使用 StringBuffer
	StringBuffer sb = new StringBuffer();
	//先把键user_gender    (用户id,性别)加进去
	sb.append(user_gender.toString());
	//遍历不同电影类型的计数         entrySet是java中键-值对的集合，entrySet实现了Set接口，里面存放的是键值对，一个K对应一个V。
	for(Entry<String, Integer> kv : genres.entrySet()) {
		sb.append("::").append(kv.getValue());
	}
	//将StringBuffer转化为String，作为键输出
	result.set(sb.toString());
	context.write(result, NullWritable.get());
	}
}
