package edu.gzgs.linrushao.dataJoin;

import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
/**
 * @Author LinRuShao
 * @Date 2023/5/26 11:30
 */
public class UARMMapper extends Mapper<LongWritable, Text,Text, NullWritable>{

    Map<String,String> movie_genres = new HashMap<>();
    Text output = new Text();
    /*
     * 读取movies.dat的数据,以moiveid为键，以电影类别为值，存入HashMap
     * */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream movie = fs.open(new Path(Contant.MOVIES));
        BufferedReader br = new BufferedReader(new InputStreamReader(movie));
        String line="";
        while ((line = br.readLine())!=null){
            String[] values = line.split("::");
            movie_genres.put(values[0],values[2]);
        }
        br.close();
    }

    /*
     * 遍历（1）中筛选出来的每一条数据，分割出moiveid，
     * 在HashMap搜索该分割出moiveid对应的值进行字符串连接，
     * 得到UserID（用户ID），Gender（性别），Genres（电影类型）
     * */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String userId = value.toString().split("::")[0];
        String user_gender = value.toString().split("::")[1];
        String moiveId = value.toString().split("::")[2];
        output.set(userId+"::"+user_gender+"::"+movie_genres.get(moiveId));
        context.write(output,NullWritable.get());
    }
}
