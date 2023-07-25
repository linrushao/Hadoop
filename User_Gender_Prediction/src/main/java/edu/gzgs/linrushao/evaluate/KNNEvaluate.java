package edu.gzgs.linrushao.evaluate;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/*
	评价分类结果准确性
 */
public class KNNEvaluate {

	public static void main(String[] args) throws IOException {
		// 配置HDFS连接参数
		Configuration conf = new Configuration();
		//定义FileSystem
		FileSystem fs = FileSystem.get(conf);
		//读取预测结果
//		String predict = "/output/user_gender_prediction/knn_output/part-r-00000";
		FSDataInputStream fin = fs.open(new Path(Contant.PREDICT_RESULT));
		BufferedReader br = new BufferedReader(new InputStreamReader(fin));
		String line=null;
		double sum=0;
		double count=0;
		while((line=br.readLine())!=null) {
			String[] lines = line.split("::");//index=0为测试类别，index=2为原始类别
			if(lines[0].equals(lines[2])) {sum++;}
			count++;

		}
		br.close();
		fin.close();
		fs.close();
		System.out.println("K=3,准确率为："+(sum/count));

	}

}
