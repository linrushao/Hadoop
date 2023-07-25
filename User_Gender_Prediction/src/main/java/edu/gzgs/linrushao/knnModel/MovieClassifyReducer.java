package edu.gzgs.linrushao.knnModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//map的输出       <测试数据,(距离，类别)>
//reduce的输入    <测试数据,((距离，类别)(距离，类别))>
//reduce的输出    <预测的标签列，测试数据> ==> <预测的性别标签列，(用户id，原始用户性别，对所有电影类型的访问次数)>
/*
	针对相同的key，即每条测试数据，对其值根据距离进行升序排序取出前K个值
	输出这K个值中的类别众数和key测试数据
 */

public class MovieClassifyReducer extends Reducer<Text, DistanceAndLabel, Text, NullWritable> {
	private int k=3;

	@Override
	protected void reduce(Text key, Iterable<DistanceAndLabel> value,
						  Reducer<Text, DistanceAndLabel, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		String label=getMost(getTopK(sort(value)));
		context.write(new Text(label+"::"+key), NullWritable.get());
	}
	/**
	 * 得到列表中类别的众数
	 * @param topK 列表中的每一个元素是：距离，类别 这种格式的字符串
	 * @return
	 */
	private String getMost(List<String> topK) {
		HashMap<String,Integer> labelTimes=new HashMap<String,Integer>();
		//遍历数据，取出类别，进行计数
		for (String str : topK) {
			String label=str.substring(str.lastIndexOf(",")+1,str.length());
			if(labelTimes.containsKey(label)){
				labelTimes.put(label, labelTimes.get(label)+1);
			}else{
				labelTimes.put(label, 1);
			}
		}
		//遍历所有的Map中的类别，根据计数进行比较，取出计数最大的类别
		int maxInt=Integer.MIN_VALUE;
		String mostLabel="";
		for(Map.Entry<String, Integer> kv:labelTimes.entrySet()){
			if(kv.getValue()>maxInt){
				maxInt=kv.getValue();
				mostLabel=kv.getKey();
			}
		}
		return mostLabel;
	}

	/**
	 * 取出列表中的前K个值
	 * @param sort 列表中的每一个元素是：距离，类别 这种格式的字符串
	 * @return
	 */
	private List<String> getTopK(List<String> sort) {
		return sort.subList(0, k);
	}

	/**
	 * 根据距离升序排序
	 * @param value
	 * @return
	 */
	private List<String> sort(Iterable<DistanceAndLabel> value) {
		//将距离和类别的数据转换成字符串存入列表
		ArrayList<String> result=new ArrayList<String>();
		for(DistanceAndLabel val:value){
			result.add(val.toString());
		}
		//将列表转化为数组
		String[] tmp=new String[result.size()];
		result.toArray(tmp);
		//利用数组的sort实现升序排序
		Arrays.sort(tmp, new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {
				double o1D=Double.parseDouble(o1.substring(0, o1.indexOf(",")));
				double o2D=Double.parseDouble(o2.substring(0, o2.indexOf(",")));
				if(o1D>o2D){
					return 1;
				}else if(o1D<o2D){
					return -1;
				}else{
					return 0;
				}
			}});
		//返回一个列表
		return Arrays.asList(tmp);
	}
}
