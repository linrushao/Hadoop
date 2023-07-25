package edu.gzgs.linrushao.dataSplit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
	按8:2的比例将数据分割成训练集、测试集
 */

public class DataSplit {

	public static void main(String[] args) throws IOException {
		// 配置连接HDFS的参数
		Configuration conf = new Configuration();
		// 指定使用的是hdfs分布式文件系统
		conf.set("fs.defaultFS","hdfs://master:8020");
		//定义FileSystem
		FileSystem fs = FileSystem.get(conf);
		//定义读取的数据路径
//		String datapath="/output/user_gender_prediction/movie_data/part-r-00000";
		//训练数据、测试数据路径
//		String trainpath="/output/user_gender_prediction/movie_train";
//		String testpath="/output/user_gender_prediction/movie_test";
		//获取记录数
		int count=getSize(fs, new Path(Contant.MOVIES_GENRES_COUNT_RESULT));
		//定义训练集的比例
		double trainpercent=0.8;
		//获取训练集下标
		Set<Integer> train_index = getIndex(count, trainpercent);
		//二次读取源数据
		FSDataInputStream fin = fs.open(new Path(Contant.MOVIES_GENRES_COUNT_RESULT)); //读取HDFS文件
		BufferedReader br = new BufferedReader(new InputStreamReader(fin));
		//遍历数据，分别写入HDFS的训练数据文件和测试数据文件
		FSDataOutputStream ftrain = fs.create(new Path(Contant.TRAIN_PATH));
		BufferedWriter bw_train = new BufferedWriter(new OutputStreamWriter(ftrain));
		FSDataOutputStream ftest = fs.create(new Path(Contant.TEST_PATH));
		BufferedWriter bw_test = new BufferedWriter(new OutputStreamWriter(ftest));
		String line=null;
		//作为下标开始
		int index=0;
		//如果读取的记录数包含在所获取的百分之八十的数据下标里面，写入训练数据文件，不包含写入测试数据文件
		while((line=br.readLine())!=null) {
			if(train_index.contains(index)) {
				bw_train.append(line);
				bw_train.newLine();
			}
			else {
				bw_test.append(line);
				bw_test.newLine();
			}
			index++;
		}
		//关闭数据流
		bw_test.close();
		bw_train.close();
		ftest.close();
		ftrain.close();
		br.close();
		fin.close();
		fs.close();
	}
	/**
	 * 读取数据文件，统计数据量
	 * @param fs 文件系统对象
	 * @param path 读取的文件路径
	 * @return 数据记录数
	 * @throws IOException
	 */
	public static int getSize(FileSystem fs,Path path) throws IOException {
		int count=0; //用来统计记录数
		FSDataInputStream fin = fs.open(path); //读取HDFS文件
		BufferedReader br = new BufferedReader(new InputStreamReader(fin));
		while(br.readLine()!=null) {
			count++;
		}
		br.close();
		fin.close();
		return count;

	}
	/**
	 *在记录数范围内，获取指定比例的随机下标
	 * @param count 总的记录条数
	 * @param percent 比例
	 * @return 指定比例的随机下标组成的集合
	 */
	public static Set<Integer> getIndex(int count,double percent){
		Set<Integer> indexs = new HashSet<>();
		int index_count = (int) (count*percent); //指定比例的下标数
		Random rd = new Random();
		while(indexs.size()<index_count) {
			indexs.add(rd.nextInt(count)); //随机取下标并添加到Set
		}
		return indexs;

	}
}
