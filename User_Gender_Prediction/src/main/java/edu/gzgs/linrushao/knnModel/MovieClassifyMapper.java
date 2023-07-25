package edu.gzgs.linrushao.knnModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
	读取每一条训练数据，计算该条训练数据与所有测试数据之间的距离
	输出<测试数据,(距离，类别)>
 */

public class MovieClassifyMapper extends Mapper<LongWritable, Text, Text, DistanceAndLabel> {
	//自定义数据类型DistanceAndLabel，封装计算出来的距离与性别标签
	private DistanceAndLabel distance_label=new DistanceAndLabel();
	ArrayList<String> testData=new ArrayList<String>();

	/*
		setup负责读取测试数据0.2的那一份数据直接读取到内存中一条一条的写入一个testData列表中
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DistanceAndLabel>.Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		//读取测试数据存于列表testData中
		FileSystem fs=FileSystem.get(conf);
		FSDataInputStream is=fs.open(new Path(Contant.TEST_PATH));
		BufferedReader br=new BufferedReader(new InputStreamReader(is));
		String line="";
		while((line=br.readLine())!=null){
			testData.add(line);
		}
		is.close();
		br.close();
	}

	/*
		map方法中读取训练数据0.8的那一份作为输入路径，
		读取到每一条训练数据的时候，都会对训练数据做一个分割，并且计算他和所有测试数据之间的距离
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DistanceAndLabel>.Context context)
			throws IOException, InterruptedException {
		double distance=0.0;
		String[] val=value.toString().split("::");
		//取出训练数据中特征列的部分
		String[] singleTrainData=Arrays.copyOfRange(val, 2, val.length);
		//取出训练数据的标签值
		String label=val[1];
		for (String td: testData) {
			String[] test=td.split("::");
			//取出测试数据的特征列部分
			String[] singleTestData=Arrays.copyOfRange(test, 2, test.length);
			//计算训练数据特征列与测试数据特征列的欧式距离
			distance=Distance(singleTrainData,singleTestData);
			distance_label.setDistance(distance);
			distance_label.setLabel(label);
			//<测试数据,(距离，类别)>
			context.write(new Text(td), distance_label);
		}
	}
	/**
	 * 计算训练数据与测试数据的距离
	 * @param singleTrainData
	 * @param singleTestData
	 * @return
	 */
	private double Distance(String[] singleTrainData, String[] singleTestData) {
		double sum=0.0;
		for(int i=0;i<singleTrainData.length;i++){
			sum+=Math.pow((Double.parseDouble(singleTrainData[i])-Double.parseDouble(singleTestData[i])),2);
		}
		return Math.sqrt(sum);
	}
}
